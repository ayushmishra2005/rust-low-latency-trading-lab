import type { SettlementAdapter } from './adapter.js';
import type { SettlementOutbox, SettlementRow } from './outbox.js';
import { ManifestConflict } from './outbox.js';

export interface DispatcherCounters {
  submitted: bigint;
  confirmed: bigint;
  alreadyApplied: bigint;
  unknown: bigint;
  rejected: bigint;
  manifestConflicts: bigint;
}

/**
 * Moves settlements through the outbox. Every attempt reuses the same
 * settlement identity and the same manifest bytes, so a retry after a timeout
 * cannot settle the same trades twice.
 */
export class SettlementDispatcher {
  readonly counters: DispatcherCounters = {
    submitted: 0n,
    confirmed: 0n,
    alreadyApplied: 0n,
    unknown: 0n,
    rejected: 0n,
    manifestConflicts: 0n,
  };

  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(
    private readonly outbox: SettlementOutbox,
    private readonly adapter: SettlementAdapter,
    private readonly batchSize = 50,
  ) {}

  start(intervalMs = 500): void {
    if (this.timer !== null) {
      return;
    }
    this.timer = setInterval(() => {
      void this.tick().catch(() => {
        // Failures are recorded on the settlement row; the loop keeps running.
      });
    }, intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async tick(): Promise<number> {
    if (this.running) {
      return 0;
    }
    this.running = true;
    try {
      this.outbox.createBatch(this.batchSize);
      const rows = this.outbox.pending(16);
      for (const row of rows) {
        await this.advance(row);
      }
      return rows.length;
    } finally {
      this.running = false;
    }
  }

  private async advance(row: SettlementRow): Promise<void> {
    let manifest;
    try {
      manifest = this.outbox.manifestFor(row.settlementId);
    } catch (error) {
      if (error instanceof ManifestConflict) {
        this.counters.manifestConflicts += 1n;
        this.outbox.recordNeedsOperator(row.settlementId, error.message);
        return;
      }
      throw error;
    }

    // An unknown outcome is re-checked before anything is sent again. Only a
    // venue that confirms it holds no receipt gets the batch a second time.
    let outcome =
      row.status === 'unknown' || row.status === 'submitted'
        ? await this.adapter.lookup(row.settlementId, row.manifestHash, manifest)
        : await this.submit(row, manifest);
    if (outcome.kind === 'notFound') {
      outcome = await this.submit(row, manifest);
    }

    switch (outcome.kind) {
      case 'confirmed':
        this.counters.confirmed += 1n;
        this.outbox.recordConfirmed(row.settlementId, outcome.receipt);
        break;
      case 'alreadyApplied':
        this.counters.alreadyApplied += 1n;
        this.outbox.recordConfirmed(row.settlementId, outcome.receipt);
        break;
      case 'unknown':
        this.counters.unknown += 1n;
        this.outbox.recordUnknown(row.settlementId, outcome.reason);
        break;
      case 'notFound':
        this.counters.unknown += 1n;
        this.outbox.recordUnknown(row.settlementId, 'venue holds no receipt');
        break;
      case 'rejected':
        this.counters.rejected += 1n;
        this.outbox.recordFailed(row.settlementId, outcome.reason);
        break;
    }
  }

  private async submit(row: SettlementRow, manifest: ReturnType<SettlementOutbox['manifestFor']>) {
    this.counters.submitted += 1n;
    this.outbox.recordAttempt(row.settlementId);
    return this.adapter.submit(manifest, row.manifestHash);
  }
}
