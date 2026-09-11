// Shapes produced by crates/control-gateway/src/json.rs. Wide integers arrive
// as decimal strings because JavaScript numbers cannot hold them exactly.

export interface TradeEvent {
  kind: 'trade';
  outputSeq: string;
  engineSeq: string;
  engineTimeNs: string;
  tradeId: string;
  instrument: number;
  makerOrderId: string;
  takerOrderId: string;
  makerAccount: number;
  takerAccount: number;
  aggressor: 'buy' | 'sell';
  priceTicks: string;
  quantityLots: string;
}

export interface ReportEvent {
  kind: 'report';
  outputSeq: string;
  engineSeq: string;
  engineTimeNs: string;
  account: number;
  instrument: number;
  requestId: string;
  clientOrderId: string;
  orderId: string;
  reportKind: string;
  orderState: string;
  side: 'buy' | 'sell';
  orderType: string;
  priceTicks: string;
  totalQuantityLots: string;
  cumulativeFilledLots: string;
  remainingLots: string;
  lastFillQuantityLots: string;
  lastFillPriceTicks: string;
  rejectReason: string | null;
}

export interface StateEvent {
  kind: 'state';
  outputSeq: string;
  engineSeq: string;
  engineTimeNs: string;
  event: { type: string } & Record<string, unknown>;
}

export type OutputEvent = TradeEvent | ReportEvent | StateEvent;

export interface BookLevel {
  side: 'buy' | 'sell';
  priceTicks: string;
  quantityLots: string;
  orderCount: number;
}

export interface InstrumentSnapshot {
  instrument: number;
  symbol: string;
  feedState: string;
  lastSourceSeq: string;
  marketBestBidTicks: string | null;
  marketBestAskTicks: string | null;
  bookLevels: BookLevel[];
  bookOrders: unknown[];
}

export interface EngineSnapshot {
  runId?: string;
  asOfEngineSeq: string;
  engineTimeNs: string;
  globalKill: boolean;
  metrics: Record<string, string>;
  instruments: InstrumentSnapshot[];
  positions: {
    account: number;
    instrument: number;
    positionLots: string;
    openBuyLots: string;
    openSellLots: string;
    openBuyNotional: string;
    openSellNotional: string;
  }[];
}

export interface GatewayHealth {
  status: string;
  runId?: string;
  uptimeSeconds: number;
  journalPath: string;
  deliveredOutputs: string;
  lastOutputSeq: string;
  asOfEngineSeq: string;
  globalKill: boolean;
  killLatched: boolean;
  telemetryDropped: string;
  feeds: { instrument: number; symbol: string; feedState: string; lastSourceSeq: string }[];
}
