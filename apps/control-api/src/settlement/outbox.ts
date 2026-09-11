import type Database from 'better-sqlite3';
import { readStateText } from '../db.js';
import { canonicalManifestBytes, manifestHash, settlementIdFor } from './manifest.js';
import type { Manifest, ManifestTrade } from './manifest.js';

/**
 * Settlement status machine.
 *
 * pending -> submitted -> confirmed
 *                     \-> unknown -> submitted | confirmed | failed
 *                     \-> unknown -> needs_operator   (retries exhausted)
 *
 * UNKNOWN is not a failure. A settlement only becomes failed when the venue
 * rejected it. needs_operator means automatic retries stopped; the economic
 * outcome is still unresolved. confirmed is terminal.
 */
export type SettlementStatus =
  | 'pending'
  | 'submitted'
  | 'unknown'
  | 'confirmed'
  | 'failed'
  | 'needs_operator';

export const MAX_AUTO_ATTEMPTS = 8;
export const BASE_BACKOFF_MS = 500;
export const MAX_BACKOFF_MS = 60_000;

export function backoffMs(attempts: number): number {
  const exp = Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS * 2 ** Math.min(Math.max(attempts, 1), 10));
  const jitter = Math.floor((exp * ((attempts * 37) % 10)) / 100);
  return exp + jitter;
}

export interface SettlementRow {
  settlementId: string;
  venue: string;
  status: SettlementStatus;
  manifestHash: string;
  firstTradeId: bigint;
  lastTradeId: bigint;
  tradeCount: bigint;
  attempts: bigint;
  nextAttemptAtMs: bigint;
  receipt: string | null;
  lastError: string | null;
}

export class ManifestConflict extends Error {
  constructor(settlementId: string) {
    super(`settlement ${settlementId} already exists with a different manifest`);
    this.name = 'ManifestConflict';
  }
}

interface RawRow {
  settlement_id: string;
  venue: string;
  status: string;
  manifest_hash: string;
  first_trade_id: bigint;
  last_trade_id: bigint;
  trade_count: bigint;
  attempts: bigint;
  next_attempt_at_ms: bigint;
  receipt: string | null;
  last_error: string | null;
}

function toRow(raw: RawRow): SettlementRow {
  return {
    settlementId: raw.settlement_id,
    venue: raw.venue,
    status: raw.status as SettlementStatus,
    manifestHash: raw.manifest_hash,
    firstTradeId: raw.first_trade_id,
    lastTradeId: raw.last_trade_id,
    tradeCount: raw.trade_count,
    attempts: raw.attempts,
    nextAttemptAtMs: raw.next_attempt_at_ms ?? 0n,
    receipt: raw.receipt,
    lastError: raw.last_error,
  };
}

export class SettlementOutbox {
  nowMs = (): number => Date.now();

  constructor(
    private readonly db: Database.Database,
    private readonly venue: string,
  ) {}

  /** Claims the next unsettled trades as one settlement. Returns null when idle. */
  createBatch(batchSize: number): SettlementRow | null {
    const build = this.db.transaction((limit: number): SettlementRow | null => {
      const rows = this.db
        .prepare(
          `SELECT trade_id, instrument, maker_account, taker_account, aggressor,
                  price_ticks, quantity_lots
             FROM trades
            WHERE settlement_id IS NULL
            ORDER BY trade_id
            LIMIT ?`,
        )
        .all(BigInt(limit)) as {
        trade_id: bigint;
        instrument: bigint;
        maker_account: bigint;
        taker_account: bigint;
        aggressor: string;
        price_ticks: bigint;
        quantity_lots: bigint;
      }[];
      if (rows.length === 0) {
        return null;
      }

      const first = rows[0]!.trade_id;
      const last = rows[rows.length - 1]!.trade_id;
      const runId = readStateText(this.db, 'run_id') || '0';
      const settlementId = settlementIdFor(this.venue, first, last, runId);
      const trades: ManifestTrade[] = rows.map((row) => ({
        tradeId: row.trade_id.toString(),
        instrument: Number(row.instrument),
        makerAccount: Number(row.maker_account),
        takerAccount: Number(row.taker_account),
        aggressor: row.aggressor as 'buy' | 'sell',
        priceTicks: row.price_ticks.toString(),
        quantityLots: row.quantity_lots.toString(),
      }));
      const manifest: Manifest = {
        version: 1,
        venue: this.venue,
        settlementId,
        firstTradeId: first.toString(),
        lastTradeId: last.toString(),
        trades,
      };
      const hash = manifestHash(manifest);

      const existing = this.get(settlementId);
      if (existing !== null && existing.manifestHash !== hash) {
        throw new ManifestConflict(settlementId);
      }

      const now = BigInt(Date.now());
      this.db
        .prepare(
          `INSERT INTO settlements
             (settlement_id, venue, status, manifest_hash, first_trade_id, last_trade_id,
              trade_count, attempts, next_attempt_at_ms, receipt, last_error, created_at_ms, updated_at_ms)
           VALUES (?, ?, 'pending', ?, ?, ?, ?, 0, 0, NULL, NULL, ?, ?)
           ON CONFLICT (settlement_id) DO NOTHING`,
        )
        .run(settlementId, this.venue, hash, first, last, BigInt(rows.length), now, now);

      this.db
        .prepare(
          `UPDATE trades SET settlement_id = ?
            WHERE settlement_id IS NULL AND trade_id BETWEEN ? AND ?`,
        )
        .run(settlementId, first, last);

      return this.get(settlementId);
    });

    return build(batchSize);
  }

  get(settlementId: string): SettlementRow | null {
    const raw = this.db
      .prepare('SELECT * FROM settlements WHERE settlement_id = ?')
      .get(settlementId) as RawRow | undefined;
    return raw === undefined ? null : toRow(raw);
  }

  /** Rebuilds the manifest from stored trades, so retries are byte-identical. */
  manifestFor(settlementId: string): Manifest {
    const row = this.get(settlementId);
    if (row === null) {
      throw new Error(`unknown settlement ${settlementId}`);
    }
    const rows = this.db
      .prepare(
        `SELECT trade_id, instrument, maker_account, taker_account, aggressor,
                price_ticks, quantity_lots
           FROM trades WHERE settlement_id = ? ORDER BY trade_id`,
      )
      .all(settlementId) as {
      trade_id: bigint;
      instrument: bigint;
      maker_account: bigint;
      taker_account: bigint;
      aggressor: string;
      price_ticks: bigint;
      quantity_lots: bigint;
    }[];
    const manifest: Manifest = {
      version: 1,
      venue: row.venue,
      settlementId,
      firstTradeId: row.firstTradeId.toString(),
      lastTradeId: row.lastTradeId.toString(),
      trades: rows.map((trade) => ({
        tradeId: trade.trade_id.toString(),
        instrument: Number(trade.instrument),
        makerAccount: Number(trade.maker_account),
        takerAccount: Number(trade.taker_account),
        aggressor: trade.aggressor as 'buy' | 'sell',
        priceTicks: trade.price_ticks.toString(),
        quantityLots: trade.quantity_lots.toString(),
      })),
    };
    if (manifestHash(manifest) !== row.manifestHash) {
      throw new ManifestConflict(settlementId);
    }
    return manifest;
  }

  manifestBytes(settlementId: string): Buffer {
    return canonicalManifestBytes(this.manifestFor(settlementId));
  }

  pending(limit: number): SettlementRow[] {
    const now = BigInt(this.nowMs());
    const raws = this.db
      .prepare(
        `SELECT * FROM settlements
          WHERE status IN ('pending', 'submitted', 'unknown')
            AND next_attempt_at_ms <= ?
          ORDER BY first_trade_id
          LIMIT ?`,
      )
      .all(now, BigInt(limit)) as RawRow[];
    return raws.map(toRow);
  }

  makeDue(settlementId?: string): void {
    if (settlementId === undefined) {
      this.db.prepare('UPDATE settlements SET next_attempt_at_ms = 0').run();
      return;
    }
    this.db
      .prepare('UPDATE settlements SET next_attempt_at_ms = 0 WHERE settlement_id = ?')
      .run(settlementId);
  }

  countByStatus(): Record<SettlementStatus, bigint> {
    const counts: Record<SettlementStatus, bigint> = {
      pending: 0n,
      submitted: 0n,
      unknown: 0n,
      confirmed: 0n,
      failed: 0n,
      needs_operator: 0n,
    };
    const rows = this.db
      .prepare('SELECT status, COUNT(*) AS total FROM settlements GROUP BY status')
      .all() as { status: string; total: bigint }[];
    for (const row of rows) {
      counts[row.status as SettlementStatus] = row.total;
    }
    return counts;
  }

  recordAttempt(settlementId: string): void {
    this.transition(settlementId, 'submitted', { nextAttemptAtMs: 0n });
  }

  recordConfirmed(settlementId: string, receipt: string): void {
    this.transition(settlementId, 'confirmed', { receipt, nextAttemptAtMs: 0n });
  }

  recordUnknown(settlementId: string, error: string): void {
    const current = this.get(settlementId);
    if (current === null) {
      throw new Error(`unknown settlement ${settlementId}`);
    }
    if (current.status === 'confirmed' || current.status === 'failed') {
      return;
    }
    const attempts = Number(current.attempts) + 1;
    if (attempts >= MAX_AUTO_ATTEMPTS) {
      this.transition(settlementId, 'needs_operator', {
        error,
        increment: true,
        nextAttemptAtMs: BigInt(Number.MAX_SAFE_INTEGER),
      });
      return;
    }
    const nextAttemptAtMs = BigInt(this.nowMs() + backoffMs(attempts));
    this.transition(settlementId, 'unknown', { error, increment: true, nextAttemptAtMs });
  }

  recordFailed(settlementId: string, error: string): void {
    this.transition(settlementId, 'failed', { error, nextAttemptAtMs: 0n });
  }

  recordNeedsOperator(settlementId: string, error: string): void {
    this.transition(settlementId, 'needs_operator', {
      error,
      nextAttemptAtMs: BigInt(Number.MAX_SAFE_INTEGER),
    });
  }

  /**
   * Proven non-economic failure only. Releases trades so they can be claimed
   * again. Never used for UNKNOWN or a confirmed settlement.
   */
  releaseFailed(settlementId: string): void {
    const current = this.get(settlementId);
    if (current === null) {
      throw new Error(`unknown settlement ${settlementId}`);
    }
    if (current.status !== 'failed') {
      throw new Error(`settlement ${settlementId} is ${current.status} and cannot be requeued`);
    }
    const release = this.db.transaction((id: string) => {
      this.db.prepare('UPDATE trades SET settlement_id = NULL WHERE settlement_id = ?').run(id);
      this.db.prepare('DELETE FROM settlements WHERE settlement_id = ?').run(id);
    });
    release(settlementId);
  }

  private transition(
    settlementId: string,
    next: SettlementStatus,
    options: {
      receipt?: string;
      error?: string;
      increment?: boolean;
      nextAttemptAtMs?: bigint;
    },
  ): void {
    const current = this.get(settlementId);
    if (current === null) {
      throw new Error(`unknown settlement ${settlementId}`);
    }
    // Confirmed is terminal. A later timeout must never undo it.
    if (current.status === 'confirmed') {
      return;
    }
    if (current.status === 'failed' && next !== 'confirmed') {
      return;
    }
    const now = BigInt(this.nowMs());
    this.db
      .prepare(
        `UPDATE settlements
            SET status = ?,
                attempts = attempts + ?,
                next_attempt_at_ms = ?,
                receipt = COALESCE(?, receipt),
                last_error = ?,
                updated_at_ms = ?
          WHERE settlement_id = ?`,
      )
      .run(
        next,
        options.increment === true ? 1n : 0n,
        options.nextAttemptAtMs ?? 0n,
        options.receipt ?? null,
        options.error ?? null,
        now,
        settlementId,
      );
  }
}
