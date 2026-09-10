//! Per-account risk state: limits, positions, working-order reservations, and a
//! bounded request deduplication window.

use protocol::{
    AccountId, AccountLimits, ExecutionReport, Notional, OrderRequest, PriceTicks, QuantityLots,
    RequestId, Side,
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentPosition {
    pub position_lots: i64,
    pub open_buy_lots: u64,
    pub open_sell_lots: u64,
    pub open_buy_notional: u128,
    pub open_sell_notional: u128,
}

impl InstrumentPosition {
    pub fn open_lots(&self, side: Side) -> u64 {
        match side {
            Side::Buy => self.open_buy_lots,
            Side::Sell => self.open_sell_lots,
        }
    }

    pub fn reserve(&mut self, side: Side, lots: QuantityLots, notional: Notional) {
        match side {
            Side::Buy => {
                self.open_buy_lots += lots.0;
                self.open_buy_notional += notional.0;
            }
            Side::Sell => {
                self.open_sell_lots += lots.0;
                self.open_sell_notional += notional.0;
            }
        }
    }

    pub fn release(&mut self, side: Side, lots: QuantityLots, notional: Notional) {
        match side {
            Side::Buy => {
                self.open_buy_lots -= lots.0;
                self.open_buy_notional -= notional.0;
            }
            Side::Sell => {
                self.open_sell_lots -= lots.0;
                self.open_sell_notional -= notional.0;
            }
        }
    }
}

/// The final report of a request, retained so an exact retry is idempotent.
pub type RequestOutcome = ExecutionReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CachedRequest {
    request_id: RequestId,
    fingerprint: u64,
    outcome: RequestOutcome,
}

/// Fixed-size ring of recent request results. Never grows.
#[derive(Debug, Clone)]
pub struct RequestCache {
    entries: Vec<Option<CachedRequest>>,
    next: usize,
}

impl RequestCache {
    pub fn new(capacity: usize) -> RequestCache {
        RequestCache {
            entries: vec![None; capacity.max(1)],
            next: 0,
        }
    }

    pub fn lookup(&self, request_id: RequestId, fingerprint: u64) -> Option<CacheHit> {
        for entry in self.entries.iter().flatten() {
            if entry.request_id == request_id {
                return Some(if entry.fingerprint == fingerprint {
                    CacheHit::Retry(entry.outcome)
                } else {
                    CacheHit::Conflict
                });
            }
        }
        None
    }

    pub fn record(&mut self, request_id: RequestId, fingerprint: u64, outcome: RequestOutcome) {
        self.entries[self.next] = Some(CachedRequest {
            request_id,
            fingerprint,
            outcome,
        });
        self.next = (self.next + 1) % self.entries.len();
    }

    /// Retained entries in insertion order. Used by the canonical state hash.
    pub fn canonical_entries(&self) -> Vec<(RequestId, u64, RequestOutcome)> {
        let len = self.entries.len();
        (0..len)
            .filter_map(|offset| self.entries[(self.next + offset) % len])
            .map(|entry| (entry.request_id, entry.fingerprint, entry.outcome))
            .collect()
    }
}

pub enum CacheHit {
    Retry(RequestOutcome),
    Conflict,
}

#[derive(Debug, Clone)]
pub struct AccountState {
    pub id: AccountId,
    pub enabled: bool,
    pub limits: AccountLimits,
    pub last_client_seq: u64,
    pub positions: Vec<InstrumentPosition>,
    pub cache: RequestCache,
}

impl AccountState {
    pub fn new(
        id: AccountId,
        enabled: bool,
        limits: AccountLimits,
        instruments: usize,
        dedup_window: usize,
    ) -> AccountState {
        AccountState {
            id,
            enabled,
            limits,
            last_client_seq: 0,
            positions: vec![InstrumentPosition::default(); instruments],
            cache: RequestCache::new(dedup_window),
        }
    }
}

/// Stable fingerprint of the economic content of a request, excluding its ID.
/// FNV-1a keeps this deterministic across runs and machines.
pub fn request_fingerprint(request: &OrderRequest) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    let mut mix = |value: u64| {
        for byte in value.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
    };
    mix(u64::from(request.kind.wire()));
    mix(u64::from(request.side.wire()));
    mix(u64::from(request.order_type.wire()));
    mix(u64::from(request.account.0));
    mix(u64::from(request.instrument.0));
    mix(request.client_order_id.0);
    mix(request.target_client_order_id.0);
    mix(request.client_seq);
    mix(request.price.0 as u64);
    mix(request.quantity.0);
    hash
}

/// Signed position after applying a fill.
pub fn apply_fill_to_position(
    position: &mut InstrumentPosition,
    side: Side,
    quantity: QuantityLots,
) -> Option<()> {
    let signed = i64::try_from(quantity.0).ok()?;
    position.position_lots = match side {
        Side::Buy => position.position_lots.checked_add(signed)?,
        Side::Sell => position.position_lots.checked_sub(signed)?,
    };
    Some(())
}

/// Absolute exposure of one instrument at the supplied mark.
pub fn instrument_exposure(position: &InstrumentPosition, mark: PriceTicks) -> Option<Notional> {
    let mark = i128::from(mark.0).unsigned_abs();
    let position_value = u128::from(position.position_lots.unsigned_abs()).checked_mul(mark)?;
    position_value
        .checked_add(position.open_buy_notional)?
        .checked_add(position.open_sell_notional)
        .map(Notional)
}
