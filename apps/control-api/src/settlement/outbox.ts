import type Database from 'better-sqlite3';
import { canonicalManifestBytes, manifestHash, settlementIdFor } from './manifest.js';
import type { Manifest, ManifestTrade } from './manifest.js';

/**
 * Settlement status machine.
 *
 * pending -> submitted -> confirmed
 *                     \-> unknown -> submitted | confirmed | failed
 *
 * UNKNOWN is not a failure. A settlement only becomes failed when the venue
 * rejected it, and confirmed is terminal.
 */
export type SettlementStatus = 'pending' | 'submitted' | 'unknown' | 'confirmed' | 'failed';

export interface SettlementRow {
  settlementId: string;
  venue: string;
  status: SettlementStatus;
  manifestHash: string;
  firstTradeId: bigint;
  lastTradeId: bigint;
  tradeCount: bigint;
  attempts: bigint;
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
    receipt: raw.receipt,
    lastError: raw.last_error,
  };
}

export class SettlementOutbox {
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
      const settlementId = settlementIdFor(this.venue, first, last);
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
              trade_count, attempts, receipt, last_error, created_at_ms, updated_at_ms)
           VALUES (?, ?, 'pending', ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
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
    const raws = this.db
      .prepare(
        `SELECT * FROM settlements
          WHERE status IN ('pending', 'submitted', 'unknown')
          ORDER BY first_trade_id
          LIMIT ?`,
      )
      .all(BigInt(limit)) as RawRow[];
    return raws.map(toRow);
  }

  countByStatus(): Record<SettlementStatus, bigint> {
    const counts: Record<SettlementStatus, bigint> = {
      pending: 0n,
      submitted: 0n,
      unknown: 0n,
      confirmed: 0n,
      failed: 0n,
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
    this.transition(settlementId, 'submitted', { attempt: true });
  }

  recordConfirmed(settlementId: string, receipt: string): void {
    this.transition(settlementId, 'confirmed', { receipt });
  }

  recordUnknown(settlementId: string, error: string): void {
    this.transition(settlementId, 'unknown', { error });
  }

  recordFailed(settlementId: string, error: string): void {
    this.transition(settlementId, 'failed', { error });
  }

  private transition(
    settlementId: string,
    next: SettlementStatus,
    options: { receipt?: string; error?: string; attempt?: boolean },
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
    this.db
      .prepare(
        `UPDATE settlements
            SET status = ?,
                attempts = attempts + ?,
                receipt = COALESCE(?, receipt),
                last_error = ?,
                updated_at_ms = ?
          WHERE settlement_id = ?`,
      )
      .run(
        next,
        options.attempt === true ? 1n : 0n,
        options.receipt ?? null,
        options.error ?? null,
        BigInt(Date.now()),
        settlementId,
      );
  }
}
