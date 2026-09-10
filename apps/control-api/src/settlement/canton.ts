import type { SettlementAdapter, SubmitOutcome } from './adapter.js';
import type { Manifest } from './manifest.js';

export interface CantonVenueOptions {
  /** Base URL of the Daml HTTP JSON API, for example http://127.0.0.1:7575. */
  baseUrl: string;
  /** Bearer token whose actAs set covers the operator and every settling party. */
  token: string;
  operator: string;
  /** Trading account id to Canton party id. */
  parties: Map<number, string>;
  /** Package id of the uploaded settlement DAR. */
  packageId: string;
  timeoutMs?: number;
}

interface Leg {
  payer: string;
  payee: string;
  amount: string;
}

interface JsonApiResult<T> {
  status: number;
  result?: T;
  errors?: string[];
}

/**
 * Settles batches through the Daml settlement workflow.
 *
 * Canton is a settlement boundary, never part of execution. The workflow keys a
 * receipt by the settlement identity, so a resubmission after a timeout is
 * recognised instead of paying twice.
 */
export class CantonVenue implements SettlementAdapter {
  readonly venue = 'canton';
  private readonly timeoutMs: number;

  constructor(private readonly options: CantonVenueOptions) {
    this.timeoutMs = options.timeoutMs ?? 15_000;
  }

  async submit(manifest: Manifest, manifestHash: string): Promise<SubmitOutcome> {
    const existing = await this.lookup(manifest.settlementId, manifestHash);
    if (existing.kind !== 'notFound') {
      return existing;
    }

    let legs: Leg[];
    let parties: string[];
    try {
      ({ legs, parties } = this.buildLegs(manifest));
    } catch (error) {
      return { kind: 'rejected', reason: message(error) };
    }

    let instruction: string;
    try {
      const created = await this.post<{ contractId: string }>('/v1/create', {
        templateId: this.templateId('SettlementInstruction'),
        payload: {
          operator: this.options.operator,
          settlementId: manifest.settlementId,
          manifestHash,
          legs,
          accepted: [],
        },
      });
      instruction = created.contractId;
    } catch (error) {
      return this.classify(error);
    }

    try {
      for (const party of parties) {
        const accepted = await this.post<{ exerciseResult: string }>('/v1/exercise', {
          templateId: this.templateId('SettlementInstruction'),
          contractId: instruction,
          choice: 'Accept',
          argument: { party },
          meta: { actAs: [party] },
        });
        instruction = accepted.exerciseResult;
      }
      const settled = await this.post<{ exerciseResult: string }>('/v1/exercise', {
        templateId: this.templateId('SettlementInstruction'),
        contractId: instruction,
        choice: 'Settle',
        argument: {},
        meta: { actAs: [this.options.operator] },
      });
      return { kind: 'confirmed', receipt: settled.exerciseResult };
    } catch (error) {
      const outcome = this.classify(error);
      if (outcome.kind === 'rejected') {
        // The ledger refused the batch, so nothing was applied. Clean up the
        // pending instruction to keep the active contract set tidy.
        await this.withdraw(instruction);
      }
      return outcome;
    }
  }

  async lookup(settlementId: string, manifestHash: string): Promise<SubmitOutcome> {
    let receipts;
    try {
      receipts = await this.post<{ contractId: string; payload: { manifestHash: string } }[]>(
        '/v1/query',
        {
          templateIds: [this.templateId('SettlementReceipt')],
          query: { operator: this.options.operator, settlementId },
        },
      );
    } catch (error) {
      return this.classify(error, 'unknown');
    }
    const receipt = receipts[0];
    if (receipt === undefined) {
      return { kind: 'notFound' };
    }
    if (receipt.payload.manifestHash !== manifestHash) {
      return { kind: 'rejected', reason: 'ledger receipt has a different manifest hash' };
    }
    return { kind: 'alreadyApplied', receipt: receipt.contractId };
  }

  private async withdraw(contractId: string): Promise<void> {
    try {
      await this.post('/v1/exercise', {
        templateId: this.templateId('SettlementInstruction'),
        contractId,
        choice: 'Withdraw',
        argument: {},
        meta: { actAs: [this.options.operator] },
      });
    } catch {
      // Best effort: a stale instruction does not affect settlement state.
    }
  }

  private classify(error: unknown, fallback: 'unknown' | 'rejected' = 'rejected'): SubmitOutcome {
    const text = message(error);
    if (/fetch failed|ECONNREFUSED|timed out|abort|502|503|504/i.test(text)) {
      return { kind: 'unknown', reason: text };
    }
    if (/DUPLICATE|already exists|UniqueKeyViolation/i.test(text)) {
      return { kind: 'unknown', reason: text };
    }
    if (/insufficient|must accept|not part of this settlement|positive|pay itself/i.test(text)) {
      return { kind: 'rejected', reason: text };
    }
    return fallback === 'unknown' ? { kind: 'unknown', reason: text } : { kind: 'rejected', reason: text };
  }

  private templateId(entity: string): string {
    return `${this.options.packageId}:Settlement:${entity}`;
  }

  private buildLegs(manifest: Manifest): { legs: Leg[]; parties: string[] } {
    const partyFor = (account: number): string => {
      const party = this.options.parties.get(account);
      if (party === undefined) {
        throw new Error(`no Canton party configured for account ${account}`);
      }
      return party;
    };

    const netted = new Map<string, Leg>();
    for (const trade of manifest.trades) {
      const amount = BigInt(trade.priceTicks) * BigInt(trade.quantityLots);
      const buyer = trade.aggressor === 'buy' ? trade.takerAccount : trade.makerAccount;
      const seller = trade.aggressor === 'buy' ? trade.makerAccount : trade.takerAccount;
      if (buyer === seller) {
        continue;
      }
      const payer = partyFor(buyer);
      const payee = partyFor(seller);
      const key = `${payer}|${payee}`;
      const existing = netted.get(key);
      if (existing === undefined) {
        netted.set(key, { payer, payee, amount: amount.toString() });
      } else {
        existing.amount = (BigInt(existing.amount) + amount).toString();
      }
    }

    const legs = [...netted.values()];
    if (legs.length === 0) {
      throw new Error('batch nets to no transfers');
    }
    const parties = [...new Set(legs.flatMap((leg) => [leg.payer, leg.payee]))];
    return { legs, parties };
  }

  private async post<T>(endpoint: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.options.baseUrl}${endpoint}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${this.options.token}`,
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    const payload = (await response.json()) as JsonApiResult<T>;
    if (!response.ok || payload.result === undefined) {
      throw new Error(payload.errors?.join('; ') ?? `ledger returned HTTP ${response.status}`);
    }
    return payload.result;
  }
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
