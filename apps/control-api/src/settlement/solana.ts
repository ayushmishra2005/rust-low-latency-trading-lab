// The Anchor package is CommonJS, so it is imported as a default binding.
import anchor from '@coral-xyz/anchor';
import type { Idl } from '@coral-xyz/anchor';

const { AnchorProvider, BN, Program, Wallet } = anchor;
import { Connection, Keypair, PublicKey } from '@solana/web3.js';
import type { SettlementAdapter, SubmitOutcome } from './adapter.js';
import type { Manifest } from './manifest.js';

const MAX_LEGS = 32;

export interface SolanaVenueOptions {
  connection: Connection;
  idl: Idl;
  programId: PublicKey;
  authority: Keypair;
  exchange: PublicKey;
  /** Trading account id to the Solana owner that holds its collateral. */
  owners: Map<number, PublicKey>;
}

/** The generated IDL types are not available here, so the call is typed narrowly. */
interface SettleBuilder {
  accounts(accounts: Record<string, PublicKey>): SettleBuilder;
  remainingAccounts(
    accounts: { pubkey: PublicKey; isWritable: boolean; isSigner: boolean }[],
  ): SettleBuilder;
  rpc(): Promise<string>;
}

type SettleBatch = (
  settlementId: number[],
  manifestHash: number[],
  legs: { payer: number; payee: number; amount: InstanceType<typeof BN> }[],
) => SettleBuilder;

interface Leg {
  payer: number;
  payee: number;
  amount: bigint;
}

/**
 * Settles batches on the local Anchor program.
 *
 * Solana is never on the matching path. A submission that does not come back
 * with a decision is reported as unknown, and the receipt PDA is the single
 * source of truth when the outcome is re-checked.
 */
export class SolanaVenue implements SettlementAdapter {
  readonly venue = 'solana';
  private readonly program: InstanceType<typeof Program>;

  constructor(private readonly options: SolanaVenueOptions) {
    const provider = new AnchorProvider(options.connection, new Wallet(options.authority), {
      commitment: 'confirmed',
    });
    this.program = new Program(options.idl, provider);
  }

  async submit(manifest: Manifest, manifestHash: string): Promise<SubmitOutcome> {
    const settlementId = settlementIdBytes(manifest.settlementId);
    const receipt = this.receiptAddress(settlementId);

    let legs: Leg[];
    let collateral: PublicKey[];
    try {
      ({ legs, collateral } = this.buildLegs(manifest));
    } catch (error) {
      return { kind: 'rejected', reason: message(error) };
    }

    try {
      const settleBatch = this.program.methods.settleBatch as unknown as SettleBatch;
      await settleBatch(
        Array.from(settlementId),
        Array.from(Buffer.from(manifestHash, 'hex')),
        legs.map((leg) => ({
          payer: leg.payer,
          payee: leg.payee,
          amount: new BN(leg.amount.toString()),
        })),
      )
        .accounts({
          authority: this.options.authority.publicKey,
          exchange: this.options.exchange,
        })
        .remainingAccounts(
          collateral.map((pubkey) => ({ pubkey, isWritable: true, isSigner: false })),
        )
        .rpc();
      return { kind: 'confirmed', receipt: receipt.toBase58() };
    } catch (error) {
      return this.classify(error, manifest.settlementId, manifestHash);
    }
  }

  async lookup(settlementId: string, manifestHash: string): Promise<SubmitOutcome> {
    const address = this.receiptAddress(settlementIdBytes(settlementId));
    let account;
    try {
      account = await this.options.connection.getAccountInfo(address, 'confirmed');
    } catch (error) {
      return { kind: 'unknown', reason: message(error) };
    }
    if (account === null) {
      return { kind: 'notFound' };
    }
    // Receipt layout: 8 byte discriminator, 32 byte exchange, 16 byte id, 32 byte hash.
    const stored = account.data.subarray(56, 88).toString('hex');
    if (stored !== manifestHash) {
      return { kind: 'rejected', reason: 'on-chain receipt has a different manifest hash' };
    }
    return { kind: 'alreadyApplied', receipt: address.toBase58() };
  }

  private async classify(
    error: unknown,
    settlementId: string,
    manifestHash: string,
  ): Promise<SubmitOutcome> {
    const text = message(error);
    // The receipt already exists, so the batch may already be applied.
    if (/already in use|custom program error: 0x0\b/.test(text)) {
      return this.lookup(settlementId, manifestHash);
    }
    if (/blockhash not found|block height exceeded|timed out|fetch failed|ECONNREFUSED|503|429/i.test(text)) {
      return { kind: 'unknown', reason: text };
    }
    if (/insufficient collateral|frozen|another exchange|greater than zero|has one constraint/i.test(text)) {
      return { kind: 'rejected', reason: text };
    }
    // An unclassified failure is not a decision.
    return { kind: 'unknown', reason: text };
  }

  private receiptAddress(settlementId: Buffer): PublicKey {
    return PublicKey.findProgramAddressSync(
      [Buffer.from('receipt'), this.options.exchange.toBuffer(), settlementId],
      this.options.programId,
    )[0];
  }

  /** Nets the batch down to one leg per debtor/creditor pair. */
  private buildLegs(manifest: Manifest): { legs: Leg[]; collateral: PublicKey[] } {
    const indexes = new Map<number, number>();
    const collateral: PublicKey[] = [];
    const indexOf = (account: number): number => {
      const existing = indexes.get(account);
      if (existing !== undefined) {
        return existing;
      }
      const owner = this.options.owners.get(account);
      if (owner === undefined) {
        throw new Error(`no Solana owner configured for account ${account}`);
      }
      const address = PublicKey.findProgramAddressSync(
        [Buffer.from('collateral'), this.options.exchange.toBuffer(), owner.toBuffer()],
        this.options.programId,
      )[0];
      const index = collateral.push(address) - 1;
      indexes.set(account, index);
      return index;
    };

    const netted = new Map<string, Leg>();
    for (const trade of manifest.trades) {
      const amount = BigInt(trade.priceTicks) * BigInt(trade.quantityLots);
      if (amount <= 0n) {
        throw new Error(`trade ${trade.tradeId} has a non-positive notional`);
      }
      const buyer = trade.aggressor === 'buy' ? trade.takerAccount : trade.makerAccount;
      const seller = trade.aggressor === 'buy' ? trade.makerAccount : trade.takerAccount;
      if (buyer === seller) {
        continue;
      }
      const payer = indexOf(buyer);
      const payee = indexOf(seller);
      const key = `${payer}:${payee}`;
      const existing = netted.get(key);
      if (existing === undefined) {
        netted.set(key, { payer, payee, amount });
      } else {
        existing.amount += amount;
      }
    }

    const legs = [...netted.values()];
    if (legs.length === 0) {
      throw new Error('batch nets to no transfers');
    }
    if (legs.length > MAX_LEGS) {
      throw new Error(`batch needs ${legs.length} legs, the program allows ${MAX_LEGS}`);
    }
    for (const leg of legs) {
      if (leg.amount > 18_446_744_073_709_551_615n) {
        throw new Error('leg amount exceeds the on-chain u64 range');
      }
    }
    return { legs, collateral };
  }
}

function settlementIdBytes(settlementId: string): Buffer {
  const hex = settlementId.replace(/^stl_/, '');
  return Buffer.from(hex.slice(0, 32), 'hex');
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
