import type { Manifest } from './manifest.js';

export type SubmitOutcome =
  | { kind: 'confirmed'; receipt: string }
  | { kind: 'alreadyApplied'; receipt: string }
  /** The venue did not answer, or answered without a decision. Retry later. */
  | { kind: 'unknown'; reason: string }
  /** The venue is reachable and holds no receipt, so the batch can be sent again. */
  | { kind: 'notFound' }
  /** The venue rejected the settlement itself. Retrying will not help. */
  | { kind: 'rejected'; reason: string };

export interface SettlementAdapter {
  readonly venue: string;
  submit(manifest: Manifest, manifestHash: string): Promise<SubmitOutcome>;
  /** Re-checks a settlement whose outcome is unknown. */
  lookup(settlementId: string, manifestHash: string): Promise<SubmitOutcome>;
}
