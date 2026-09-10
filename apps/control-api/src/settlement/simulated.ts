import { createHash } from 'node:crypto';
import type { SettlementAdapter, SubmitOutcome } from './adapter.js';
import type { Manifest } from './manifest.js';

export interface SimulatedFaults {
  /** The venue is unreachable. Every call reports an unknown outcome. */
  unavailable: boolean;
  /** The submit call times out after the venue already applied the batch. */
  timeoutAfterApply: boolean;
  /** The venue rejects the batch outright. */
  reject: boolean;
}

/**
 * In-process settlement venue used for local runs and failure injection. It
 * behaves like a ledger that keys receipts by settlement identity, so a repeat
 * submission of the same identity returns the original receipt.
 */
export class SimulatedVenue implements SettlementAdapter {
  readonly venue = 'simulated';
  readonly faults: SimulatedFaults = {
    unavailable: false,
    timeoutAfterApply: false,
    reject: false,
  };

  private readonly applied = new Map<string, { receipt: string; manifestHash: string }>();

  async submit(manifest: Manifest, manifestHash: string): Promise<SubmitOutcome> {
    if (this.faults.unavailable) {
      return { kind: 'unknown', reason: 'venue unavailable' };
    }
    if (this.faults.reject) {
      return { kind: 'rejected', reason: 'venue rejected the batch' };
    }

    const existing = this.applied.get(manifest.settlementId);
    if (existing !== undefined) {
      if (existing.manifestHash !== manifestHash) {
        return { kind: 'rejected', reason: 'settlement id already applied with another manifest' };
      }
      return { kind: 'alreadyApplied', receipt: existing.receipt };
    }

    const receipt = createHash('sha256')
      .update(`${manifest.settlementId}|${manifestHash}`)
      .digest('hex')
      .slice(0, 32);
    this.applied.set(manifest.settlementId, { receipt, manifestHash });

    if (this.faults.timeoutAfterApply) {
      // The ledger applied it but the caller never learned the result.
      return { kind: 'unknown', reason: 'no response after submission' };
    }
    return { kind: 'confirmed', receipt };
  }

  async lookup(settlementId: string, manifestHash: string): Promise<SubmitOutcome> {
    if (this.faults.unavailable) {
      return { kind: 'unknown', reason: 'venue unavailable' };
    }
    const existing = this.applied.get(settlementId);
    if (existing === undefined) {
      return { kind: 'notFound' };
    }
    if (existing.manifestHash !== manifestHash) {
      return { kind: 'rejected', reason: 'receipt manifest does not match the local manifest' };
    }
    return { kind: 'alreadyApplied', receipt: existing.receipt };
  }
}
