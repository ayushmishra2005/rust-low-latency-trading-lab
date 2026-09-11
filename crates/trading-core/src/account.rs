//! Per-account risk state: limits, positions, working-order reservations, and a
//! bounded request deduplication window.

use std::collections::HashMap;

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

    pub fn reserve(&mut self, side: Side, lots: QuantityLots, notional: Notional) -> Option<()> {
        match side {
            Side::Buy => {
                self.open_buy_lots = self.open_buy_lots.checked_add(lots.0)?;
                self.open_buy_notional = self.open_buy_notional.checked_add(notional.0)?;
            }
            Side::Sell => {
                self.open_sell_lots = self.open_sell_lots.checked_add(lots.0)?;
                self.open_sell_notional = self.open_sell_notional.checked_add(notional.0)?;
            }
        }
        Some(())
    }

    pub fn release(&mut self, side: Side, lots: QuantityLots, notional: Notional) -> Option<()> {
        match side {
            Side::Buy => {
                self.open_buy_lots = self.open_buy_lots.checked_sub(lots.0)?;
                self.open_buy_notional = self.open_buy_notional.checked_sub(notional.0)?;
            }
            Side::Sell => {
                self.open_sell_lots = self.open_sell_lots.checked_sub(lots.0)?;
                self.open_sell_notional = self.open_sell_notional.checked_sub(notional.0)?;
            }
        }
        Some(())
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

/// Fixed-size ring of recent request results. Lookup is O(1) via the index;
/// the ring still bounds memory and eviction order.
#[derive(Debug, Clone)]
pub struct RequestCache {
    entries: Vec<Option<CachedRequest>>,
    /// RequestId -> ring slot. Never used for hashing or replay order.
    index: HashMap<RequestId, usize>,
    next: usize,
}

impl RequestCache {
    pub fn new(capacity: usize) -> RequestCache {
        let capacity = capacity.max(1);
        RequestCache {
            entries: vec![None; capacity],
            index: HashMap::with_capacity(capacity),
            next: 0,
        }
    }

    pub fn lookup(&self, request_id: RequestId, fingerprint: u64) -> Option<CacheHit> {
        let slot = *self.index.get(&request_id)?;
        let entry = self.entries.get(slot)?.as_ref()?;
        if entry.request_id != request_id {
            // Slot was reused; a stale index must not resolve.
            return None;
        }
        Some(if entry.fingerprint == fingerprint {
            CacheHit::Retry(entry.outcome)
        } else {
            CacheHit::Conflict
        })
    }

    pub fn record(&mut self, request_id: RequestId, fingerprint: u64, outcome: RequestOutcome) {
        if let Some(previous) = self.entries[self.next] {
            if self.index.get(&previous.request_id) == Some(&self.next) {
                self.index.remove(&previous.request_id);
            }
        }
        self.entries[self.next] = Some(CachedRequest {
            request_id,
            fingerprint,
            outcome,
        });
        self.index.insert(request_id, self.next);
        self.next = (self.next + 1) % self.entries.len();
    }

    #[cfg(test)]
    pub fn force_stale_index(&mut self, request_id: RequestId, slot: usize) {
        self.index.insert(request_id, slot);
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

#[derive(Debug)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{
        AccountId, ClientOrderId, EngineSeq, InstrumentId, OrderId, OrderState, OrderType,
        OutputSeq, ReportKind,
    };

    fn report(order_id: u64) -> RequestOutcome {
        RequestOutcome {
            output_seq: OutputSeq(order_id),
            engine_seq: EngineSeq(order_id),
            engine_time_ns: 1,
            account: AccountId(1),
            instrument: InstrumentId(1),
            request_id: RequestId(order_id),
            client_order_id: ClientOrderId(order_id),
            order_id: OrderId(order_id),
            kind: ReportKind::Accepted,
            state: OrderState::Working,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: PriceTicks(99),
            total_quantity: QuantityLots(1),
            cumulative_filled: QuantityLots(0),
            remaining: QuantityLots(1),
            last_fill_quantity: QuantityLots(0),
            last_fill_price: PriceTicks(0),
            reject_reason: None,
        }
    }

    #[test]
    fn exact_retry_and_conflicting_duplicate() {
        let mut cache = RequestCache::new(8);
        cache.record(RequestId(7), 100, report(7));
        match cache.lookup(RequestId(7), 100) {
            Some(CacheHit::Retry(outcome)) => assert_eq!(outcome.order_id, OrderId(7)),
            other => panic!("expected retry, got {other:?}"),
        }
        assert!(matches!(
            cache.lookup(RequestId(7), 101),
            Some(CacheHit::Conflict)
        ));
    }

    #[test]
    fn eviction_drops_the_old_index() {
        let mut cache = RequestCache::new(2);
        cache.record(RequestId(1), 1, report(1));
        cache.record(RequestId(2), 2, report(2));
        cache.record(RequestId(3), 3, report(3));
        assert!(cache.lookup(RequestId(1), 1).is_none());
        assert!(matches!(
            cache.lookup(RequestId(2), 2),
            Some(CacheHit::Retry(_))
        ));
        assert!(matches!(
            cache.lookup(RequestId(3), 3),
            Some(CacheHit::Retry(_))
        ));
    }

    #[test]
    fn slot_reuse_does_not_resolve_the_evicted_id() {
        let mut cache = RequestCache::new(1);
        cache.record(RequestId(1), 11, report(1));
        cache.record(RequestId(2), 22, report(2));
        assert!(cache.lookup(RequestId(1), 11).is_none());
        match cache.lookup(RequestId(2), 22) {
            Some(CacheHit::Retry(outcome)) => assert_eq!(outcome.request_id, RequestId(2)),
            other => panic!("expected the reused slot to hold request 2, got {other:?}"),
        }
    }

    #[test]
    fn a_stale_index_entry_does_not_resolve() {
        let mut cache = RequestCache::new(2);
        cache.record(RequestId(1), 1, report(1));
        cache.force_stale_index(RequestId(99), 0);
        assert!(cache.lookup(RequestId(99), 1).is_none());
        assert!(matches!(
            cache.lookup(RequestId(1), 1),
            Some(CacheHit::Retry(_))
        ));
    }

    #[test]
    fn canonical_entries_follow_ring_order_not_hash_iteration() {
        let mut cache = RequestCache::new(4);
        for id in [10u64, 20, 30] {
            cache.record(RequestId(id), id, report(id));
        }
        let first: Vec<u64> = cache
            .canonical_entries()
            .iter()
            .map(|(id, _, _)| id.0)
            .collect();
        let second: Vec<u64> = cache
            .canonical_entries()
            .iter()
            .map(|(id, _, _)| id.0)
            .collect();
        assert_eq!(first, vec![10, 20, 30]);
        assert_eq!(first, second);

        let mut other = RequestCache::new(4);
        for id in [10u64, 20, 30] {
            other.record(RequestId(id), id, report(id));
        }
        assert_eq!(cache.canonical_entries(), other.canonical_entries());
    }

    #[test]
    fn reserve_and_fill_arithmetic_is_checked() {
        let mut position = InstrumentPosition {
            open_buy_lots: 1,
            ..InstrumentPosition::default()
        };
        assert!(position
            .reserve(Side::Buy, QuantityLots(u64::MAX), Notional(1))
            .is_none());
        assert!(position
            .release(Side::Buy, QuantityLots(2), Notional(0))
            .is_none());

        let mut filled = InstrumentPosition {
            position_lots: i64::MAX,
            ..InstrumentPosition::default()
        };
        assert!(apply_fill_to_position(&mut filled, Side::Buy, QuantityLots(1)).is_none());
        filled.position_lots = i64::MIN;
        assert!(apply_fill_to_position(&mut filled, Side::Sell, QuantityLots(1)).is_none());
    }
}
