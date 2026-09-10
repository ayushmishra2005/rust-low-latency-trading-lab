import { createHash } from 'node:crypto';

export interface ManifestTrade {
  tradeId: string;
  instrument: number;
  makerAccount: number;
  takerAccount: number;
  aggressor: 'buy' | 'sell';
  priceTicks: string;
  quantityLots: string;
}

export interface Manifest {
  version: 1;
  venue: string;
  settlementId: string;
  firstTradeId: string;
  lastTradeId: string;
  trades: ManifestTrade[];
}

/**
 * Canonical bytes for hashing and for submission. Field order is fixed here, so
 * the same batch always hashes to the same value on every machine.
 */
export function canonicalManifestBytes(manifest: Manifest): Buffer {
  const trades = manifest.trades.map((trade) => [
    trade.tradeId,
    trade.instrument,
    trade.makerAccount,
    trade.takerAccount,
    trade.aggressor,
    trade.priceTicks,
    trade.quantityLots,
  ]);
  const canonical = [
    manifest.version,
    manifest.venue,
    manifest.settlementId,
    manifest.firstTradeId,
    manifest.lastTradeId,
    trades,
  ];
  return Buffer.from(JSON.stringify(canonical), 'utf8');
}

export function manifestHash(manifest: Manifest): string {
  return createHash('sha256').update(canonicalManifestBytes(manifest)).digest('hex');
}

/**
 * Business identity of a settlement. Derived from the trade range, so a retry
 * after a timeout resubmits the same identity instead of creating a new one.
 */
export function settlementIdFor(venue: string, firstTradeId: bigint, lastTradeId: bigint): string {
  const digest = createHash('sha256')
    .update(`${venue}|${firstTradeId.toString()}|${lastTradeId.toString()}`)
    .digest('hex');
  return `stl_${digest.slice(0, 32)}`;
}
