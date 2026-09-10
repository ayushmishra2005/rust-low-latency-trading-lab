import type Database from 'better-sqlite3';
import type { GatewayClient } from './gateway.js';
import { readState, writeState } from './db.js';
import type { EngineSnapshot, OutputEvent, ReportEvent, TradeEvent } from './types.js';

export interface ProjectionCounters {
  eventsApplied: bigint;
  tradesApplied: bigint;
  reportsApplied: bigint;
  riskRejects: bigint;
  gapsObserved: bigint;
  pollErrors: bigint;
}

export type ProjectionListener = (event: OutputEvent, projectionSeq: bigint) => void;

/**
 * Applies the engine output stream to SQLite. The engine is the source of
 * truth; this is a lagging read model and always reports how far it has read.
 */
export class Projection {
  readonly counters: ProjectionCounters = {
    eventsApplied: 0n,
    tradesApplied: 0n,
    reportsApplied: 0n,
    riskRejects: 0n,
    gapsObserved: 0n,
    pollErrors: 0n,
  };

  private lastOutputSeq: bigint;
  private projectionSeq: bigint;
  private lastEngineSeq: bigint;
  private listeners: ProjectionListener[] = [];
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(
    private readonly db: Database.Database,
    private readonly gateway: GatewayClient,
    private readonly batchSize = 500,
  ) {
    this.lastOutputSeq = readState(db, 'last_output_seq');
    this.projectionSeq = readState(db, 'projection_seq');
    this.lastEngineSeq = readState(db, 'as_of_engine_seq');
  }

  get position(): { lastOutputSeq: bigint; projectionSeq: bigint; asOfEngineSeq: bigint } {
    return {
      lastOutputSeq: this.lastOutputSeq,
      projectionSeq: this.projectionSeq,
      asOfEngineSeq: this.lastEngineSeq,
    };
  }

  onEvent(listener: ProjectionListener): void {
    this.listeners.push(listener);
  }

  offEvent(listener: ProjectionListener): void {
    this.listeners = this.listeners.filter((entry) => entry !== listener);
  }

  start(intervalMs = 100): void {
    if (this.timer !== null) {
      return;
    }
    this.timer = setInterval(() => {
      void this.poll();
    }, intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /** Reads one batch. Returns how many events were applied. */
  async poll(): Promise<number> {
    if (this.running) {
      return 0;
    }
    this.running = true;
    try {
      const result = await this.gateway.call<{ events: OutputEvent[] }>('outputs', {
        limit: this.batchSize,
      });
      if (result.events.length === 0) {
        return 0;
      }
      this.apply(result.events);
      return result.events.length;
    } catch (error) {
      this.counters.pollErrors += 1n;
      throw error;
    } finally {
      this.running = false;
    }
  }

  private apply(events: OutputEvent[]): void {
    const applied: { event: OutputEvent; projectionSeq: bigint }[] = [];
    const applyBatch = this.db.transaction((batch: OutputEvent[]) => {
      for (const event of batch) {
        const outputSeq = BigInt(event.outputSeq);
        if (outputSeq <= this.lastOutputSeq) {
          continue;
        }
        if (outputSeq !== this.lastOutputSeq + 1n && this.lastOutputSeq !== 0n) {
          // A gap means the journal was replaced or a read was missed. Record it;
          // consumers resnapshot instead of trusting a partial stream.
          this.counters.gapsObserved += 1n;
        }
        switch (event.kind) {
          case 'trade':
            this.applyTrade(event);
            this.counters.tradesApplied += 1n;
            break;
          case 'report':
            this.applyReport(event);
            this.counters.reportsApplied += 1n;
            break;
          case 'state':
            break;
        }
        this.lastOutputSeq = outputSeq;
        this.lastEngineSeq = BigInt(event.engineSeq);
        this.projectionSeq += 1n;
        this.counters.eventsApplied += 1n;
        applied.push({ event, projectionSeq: this.projectionSeq });
      }
      writeState(this.db, 'last_output_seq', this.lastOutputSeq);
      writeState(this.db, 'as_of_engine_seq', this.lastEngineSeq);
      writeState(this.db, 'projection_seq', this.projectionSeq);
    });

    applyBatch(events);

    for (const entry of applied) {
      for (const listener of this.listeners) {
        listener(entry.event, entry.projectionSeq);
      }
    }
  }

  private applyTrade(event: TradeEvent): void {
    this.db
      .prepare(
        `INSERT OR IGNORE INTO trades
           (trade_id, output_seq, engine_seq, engine_time_ns, instrument, maker_account,
            taker_account, aggressor, price_ticks, quantity_lots, settlement_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
      )
      .run(
        BigInt(event.tradeId),
        BigInt(event.outputSeq),
        BigInt(event.engineSeq),
        BigInt(event.engineTimeNs),
        BigInt(event.instrument),
        BigInt(event.makerAccount),
        BigInt(event.takerAccount),
        event.aggressor,
        BigInt(event.priceTicks),
        BigInt(event.quantityLots),
      );

    const quantity = BigInt(event.quantityLots);
    const takerBuys = event.aggressor === 'buy';
    this.addPosition(event.takerAccount, event.instrument, takerBuys ? quantity : -quantity);
    this.addPosition(event.makerAccount, event.instrument, takerBuys ? -quantity : quantity);
  }

  private addPosition(account: number, instrument: number, delta: bigint): void {
    this.db
      .prepare(
        `INSERT INTO positions (account, instrument, position_lots, bought_lots, sold_lots)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (account, instrument) DO UPDATE SET
           position_lots = position_lots + excluded.position_lots,
           bought_lots   = bought_lots + excluded.bought_lots,
           sold_lots     = sold_lots + excluded.sold_lots`,
      )
      .run(
        BigInt(account),
        BigInt(instrument),
        delta,
        delta > 0n ? delta : 0n,
        delta < 0n ? -delta : 0n,
      );
  }

  private applyReport(event: ReportEvent): void {
    if (event.rejectReason !== null) {
      this.db
        .prepare(
          `INSERT INTO risk_rejects (reason, count) VALUES (?, 1)
           ON CONFLICT (reason) DO UPDATE SET count = count + 1`,
        )
        .run(event.rejectReason);
      this.counters.riskRejects += 1n;
      return;
    }
    if (event.orderId === '0') {
      return;
    }

    const terminal = ['filled', 'cancelled', 'rejected'].includes(event.orderState);
    if (terminal) {
      this.db.prepare('DELETE FROM orders WHERE order_id = ?').run(BigInt(event.orderId));
      return;
    }

    this.db
      .prepare(
        `INSERT INTO orders
           (order_id, account, instrument, client_order_id, side, order_type,
            price_ticks, total_lots, filled_lots, state, updated_output_seq)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (order_id) DO UPDATE SET
           price_ticks = excluded.price_ticks,
           total_lots = excluded.total_lots,
           filled_lots = excluded.filled_lots,
           state = excluded.state,
           updated_output_seq = excluded.updated_output_seq`,
      )
      .run(
        BigInt(event.orderId),
        BigInt(event.account),
        BigInt(event.instrument),
        BigInt(event.clientOrderId),
        event.side,
        event.orderType,
        BigInt(event.priceTicks),
        BigInt(event.totalQuantityLots),
        BigInt(event.cumulativeFilledLots),
        event.orderState,
        BigInt(event.outputSeq),
      );
  }
}

/** Book depth always comes from the engine snapshot, never from the projection. */
export async function fetchSnapshot(gateway: GatewayClient): Promise<EngineSnapshot> {
  return gateway.call<EngineSnapshot>('snapshot');
}
