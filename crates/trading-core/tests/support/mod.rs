//! Shared fixtures for the engine tests.

use protocol::{
    AccountId, ClientOrderId, EngineInput, IngressSeq, InputEvent, InstrumentId, MarketEvent,
    MarketEventKind, OrderRequest, OrderType, OutputEvent, PriceTicks, QuantityLots, RequestId,
    RequestKind, Side,
};
use trading_core::{EngineConfig, TradingCore};

pub const INSTRUMENT: InstrumentId = InstrumentId(1);

pub struct Harness {
    pub core: TradingCore,
    pub output: Vec<OutputEvent>,
    pub events: Vec<OutputEvent>,
    ingress: u64,
    source_seq: u64,
    time_ns: u64,
    request_id: u64,
    client_seq: [u64; 8],
}

impl Harness {
    pub fn new() -> Harness {
        Harness::with_config(EngineConfig::single_instrument(7))
    }

    pub fn with_config(config: EngineConfig) -> Harness {
        Harness {
            core: TradingCore::new(config),
            output: Vec::new(),
            events: Vec::new(),
            ingress: 0,
            source_seq: 0,
            time_ns: 1_000,
            request_id: 0,
            client_seq: [0; 8],
        }
    }

    fn next_input(&mut self, event: InputEvent) -> EngineInput {
        self.ingress += 1;
        self.time_ns += 1_000;
        EngineInput {
            ingress_seq: IngressSeq(self.ingress),
            recv_time_ns: self.time_ns,
            event,
        }
    }

    pub fn apply(&mut self, event: InputEvent) -> &[OutputEvent] {
        let input = self.next_input(event);
        self.core.apply(&input, &mut self.output);
        self.events.extend_from_slice(&self.output);
        &self.output
    }

    pub fn market(&mut self, kind: MarketEventKind) -> &[OutputEvent] {
        self.source_seq += 1;
        let event = InputEvent::Market(MarketEvent {
            instrument: INSTRUMENT,
            source_seq: self.source_seq,
            source_time_ns: self.time_ns,
            kind,
        });
        self.apply(event)
    }

    /// Brings the feed to `Live` with a 99/101 book.
    pub fn sync_feed(&mut self) {
        self.market(MarketEventKind::SnapshotBegin { snapshot_seq: 1 });
        self.market(MarketEventKind::SnapshotLevel {
            side: Side::Buy,
            price: PriceTicks(99),
            quantity: QuantityLots(100),
        });
        self.market(MarketEventKind::SnapshotLevel {
            side: Side::Sell,
            price: PriceTicks(101),
            quantity: QuantityLots(100),
        });
        self.market(MarketEventKind::SnapshotEnd {
            snapshot_seq: 1,
            level_count: 2,
        });
    }

    fn request(&mut self, account: u32, kind: RequestKind) -> OrderRequest {
        self.request_id += 1;
        let slot = account as usize;
        self.client_seq[slot] += 1;
        OrderRequest {
            kind,
            account: AccountId(account),
            instrument: INSTRUMENT,
            request_id: RequestId(self.request_id),
            client_seq: self.client_seq[slot],
            client_order_id: ClientOrderId(0),
            target_client_order_id: ClientOrderId(0),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: PriceTicks(0),
            quantity: QuantityLots(0),
        }
    }

    pub fn limit(
        &mut self,
        account: u32,
        client_order_id: u64,
        side: Side,
        price: i64,
        quantity: u64,
    ) -> &[OutputEvent] {
        let mut request = self.request(account, RequestKind::New);
        request.client_order_id = ClientOrderId(client_order_id);
        request.side = side;
        request.order_type = OrderType::Limit;
        request.price = PriceTicks(price);
        request.quantity = QuantityLots(quantity);
        self.apply(InputEvent::Order(request))
    }

    pub fn market_order(
        &mut self,
        account: u32,
        client_order_id: u64,
        side: Side,
        quantity: u64,
    ) -> &[OutputEvent] {
        let mut request = self.request(account, RequestKind::New);
        request.client_order_id = ClientOrderId(client_order_id);
        request.side = side;
        request.order_type = OrderType::Market;
        request.quantity = QuantityLots(quantity);
        self.apply(InputEvent::Order(request))
    }

    pub fn cancel(&mut self, account: u32, target: u64) -> &[OutputEvent] {
        let mut request = self.request(account, RequestKind::Cancel);
        request.target_client_order_id = ClientOrderId(target);
        self.apply(InputEvent::Order(request))
    }

    pub fn replace(
        &mut self,
        account: u32,
        target: u64,
        side: Side,
        price: i64,
        new_total: u64,
    ) -> &[OutputEvent] {
        self.replace_as(account, target, side, OrderType::Limit, price, new_total)
    }

    pub fn replace_as(
        &mut self,
        account: u32,
        target: u64,
        side: Side,
        order_type: OrderType,
        price: i64,
        new_total: u64,
    ) -> &[OutputEvent] {
        let mut request = self.request(account, RequestKind::Replace);
        request.target_client_order_id = ClientOrderId(target);
        request.side = side;
        request.order_type = order_type;
        request.price = PriceTicks(price);
        request.quantity = QuantityLots(new_total);
        self.apply(InputEvent::Order(request))
    }

    pub fn control(&mut self, command: protocol::ControlCommand) -> &[OutputEvent] {
        self.apply(InputEvent::Control(command))
    }

    pub fn check_invariants(&self) {
        for instrument in self.core.instruments() {
            instrument
                .book
                .check_invariants()
                .unwrap_or_else(|error| panic!("book invariant broken: {error}"));
        }
    }
}

pub fn reports(events: &[OutputEvent]) -> Vec<protocol::ExecutionReport> {
    events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Report(report) => Some(*report),
            _ => None,
        })
        .collect()
}

pub fn trades(events: &[OutputEvent]) -> Vec<protocol::TradeEvent> {
    events
        .iter()
        .filter_map(|event| match event {
            OutputEvent::Trade(trade) => Some(*trade),
            _ => None,
        })
        .collect()
}

pub fn last_report(events: &[OutputEvent]) -> protocol::ExecutionReport {
    *reports(events).last().expect("expected a report")
}
