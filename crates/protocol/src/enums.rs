//! Closed enums used on the wire and inside the engine. Wire values are stable.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub const fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    pub const fn wire(self) -> u8 {
        match self {
            Side::Buy => 1,
            Side::Sell => 2,
        }
    }

    pub const fn from_wire(value: u8) -> Option<Side> {
        match value {
            1 => Some(Side::Buy),
            2 => Some(Side::Sell),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderType {
    /// Good till cancel.
    Limit,
    /// Immediate or cancel, bounded by a protection price.
    Market,
}

impl OrderType {
    pub const fn wire(self) -> u8 {
        match self {
            OrderType::Limit => 1,
            OrderType::Market => 2,
        }
    }

    pub const fn from_wire(value: u8) -> Option<OrderType> {
        match value {
            1 => Some(OrderType::Limit),
            2 => Some(OrderType::Market),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderState {
    Working,
    PartiallyFilled,
    Filled,
    Cancelled,
}

impl OrderState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, OrderState::Filled | OrderState::Cancelled)
    }

    pub const fn wire(self) -> u8 {
        match self {
            OrderState::Working => 1,
            OrderState::PartiallyFilled => 2,
            OrderState::Filled => 3,
            OrderState::Cancelled => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestKind {
    New,
    Cancel,
    Replace,
}

impl RequestKind {
    pub const fn wire(self) -> u8 {
        match self {
            RequestKind::New => 1,
            RequestKind::Cancel => 2,
            RequestKind::Replace => 3,
        }
    }

    pub const fn from_wire(value: u8) -> Option<RequestKind> {
        match value {
            1 => Some(RequestKind::New),
            2 => Some(RequestKind::Cancel),
            3 => Some(RequestKind::Replace),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReportKind {
    Accepted,
    Rejected,
    PartiallyFilled,
    Filled,
    Cancelled,
    Replaced,
}

impl ReportKind {
    pub const fn wire(self) -> u8 {
        match self {
            ReportKind::Accepted => 1,
            ReportKind::Rejected => 2,
            ReportKind::PartiallyFilled => 3,
            ReportKind::Filled => 4,
            ReportKind::Cancelled => 5,
            ReportKind::Replaced => 6,
        }
    }
}

/// Closed reject set. Values are stable because they appear in golden outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectReason {
    MalformedRequest,
    UnknownInstrument,
    UnknownAccount,
    UnknownOrder,
    InvalidSide,
    InvalidOrderType,
    InvalidPrice,
    InvalidQuantity,
    PriceNotAligned,
    QuantityNotAligned,
    DuplicateRequestConflict,
    SequenceTooOld,
    SequenceOutOfOrder,
    DuplicateClientOrderId,
    AccountDisabled,
    GlobalKillActive,
    MarketDataUnsynchronized,
    MarketDataStale,
    MaxOrderQuantity,
    MaxOrderNotional,
    PriceCollar,
    MaxPosition,
    MaxGrossExposure,
    CapacityExhausted,
    ArithmeticOverflow,
    OrderAlreadyTerminal,
    InvalidReplaceQuantity,
    NoReferencePrice,
}

impl RejectReason {
    pub const fn wire(self) -> u8 {
        match self {
            RejectReason::MalformedRequest => 1,
            RejectReason::UnknownInstrument => 2,
            RejectReason::UnknownAccount => 3,
            RejectReason::UnknownOrder => 4,
            RejectReason::InvalidSide => 5,
            RejectReason::InvalidOrderType => 6,
            RejectReason::InvalidPrice => 7,
            RejectReason::InvalidQuantity => 8,
            RejectReason::PriceNotAligned => 9,
            RejectReason::QuantityNotAligned => 10,
            RejectReason::DuplicateRequestConflict => 11,
            RejectReason::SequenceTooOld => 12,
            RejectReason::SequenceOutOfOrder => 13,
            RejectReason::DuplicateClientOrderId => 14,
            RejectReason::AccountDisabled => 15,
            RejectReason::GlobalKillActive => 16,
            RejectReason::MarketDataUnsynchronized => 17,
            RejectReason::MarketDataStale => 18,
            RejectReason::MaxOrderQuantity => 19,
            RejectReason::MaxOrderNotional => 20,
            RejectReason::PriceCollar => 21,
            RejectReason::MaxPosition => 22,
            RejectReason::MaxGrossExposure => 23,
            RejectReason::CapacityExhausted => 24,
            RejectReason::ArithmeticOverflow => 25,
            RejectReason::OrderAlreadyTerminal => 26,
            RejectReason::InvalidReplaceQuantity => 27,
            RejectReason::NoReferencePrice => 28,
        }
    }

    /// Short stable label for bounded metric labels and logs.
    pub const fn label(self) -> &'static str {
        match self {
            RejectReason::MalformedRequest => "malformed_request",
            RejectReason::UnknownInstrument => "unknown_instrument",
            RejectReason::UnknownAccount => "unknown_account",
            RejectReason::UnknownOrder => "unknown_order",
            RejectReason::InvalidSide => "invalid_side",
            RejectReason::InvalidOrderType => "invalid_order_type",
            RejectReason::InvalidPrice => "invalid_price",
            RejectReason::InvalidQuantity => "invalid_quantity",
            RejectReason::PriceNotAligned => "price_not_aligned",
            RejectReason::QuantityNotAligned => "quantity_not_aligned",
            RejectReason::DuplicateRequestConflict => "duplicate_request_conflict",
            RejectReason::SequenceTooOld => "sequence_too_old",
            RejectReason::SequenceOutOfOrder => "sequence_out_of_order",
            RejectReason::DuplicateClientOrderId => "duplicate_client_order_id",
            RejectReason::AccountDisabled => "account_disabled",
            RejectReason::GlobalKillActive => "global_kill_active",
            RejectReason::MarketDataUnsynchronized => "market_data_unsynchronized",
            RejectReason::MarketDataStale => "market_data_stale",
            RejectReason::MaxOrderQuantity => "max_order_quantity",
            RejectReason::MaxOrderNotional => "max_order_notional",
            RejectReason::PriceCollar => "price_collar",
            RejectReason::MaxPosition => "max_position",
            RejectReason::MaxGrossExposure => "max_gross_exposure",
            RejectReason::CapacityExhausted => "capacity_exhausted",
            RejectReason::ArithmeticOverflow => "arithmetic_overflow",
            RejectReason::OrderAlreadyTerminal => "order_already_terminal",
            RejectReason::InvalidReplaceQuantity => "invalid_replace_quantity",
            RejectReason::NoReferencePrice => "no_reference_price",
        }
    }

    pub const ALL: [RejectReason; 28] = [
        RejectReason::MalformedRequest,
        RejectReason::UnknownInstrument,
        RejectReason::UnknownAccount,
        RejectReason::UnknownOrder,
        RejectReason::InvalidSide,
        RejectReason::InvalidOrderType,
        RejectReason::InvalidPrice,
        RejectReason::InvalidQuantity,
        RejectReason::PriceNotAligned,
        RejectReason::QuantityNotAligned,
        RejectReason::DuplicateRequestConflict,
        RejectReason::SequenceTooOld,
        RejectReason::SequenceOutOfOrder,
        RejectReason::DuplicateClientOrderId,
        RejectReason::AccountDisabled,
        RejectReason::GlobalKillActive,
        RejectReason::MarketDataUnsynchronized,
        RejectReason::MarketDataStale,
        RejectReason::MaxOrderQuantity,
        RejectReason::MaxOrderNotional,
        RejectReason::PriceCollar,
        RejectReason::MaxPosition,
        RejectReason::MaxGrossExposure,
        RejectReason::CapacityExhausted,
        RejectReason::ArithmeticOverflow,
        RejectReason::OrderAlreadyTerminal,
        RejectReason::InvalidReplaceQuantity,
        RejectReason::NoReferencePrice,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedState {
    Unsynchronized,
    ApplyingSnapshot,
    Live,
    Gap,
}

impl FeedState {
    pub const fn wire(self) -> u8 {
        match self {
            FeedState::Unsynchronized => 1,
            FeedState::ApplyingSnapshot => 2,
            FeedState::Live => 3,
            FeedState::Gap => 4,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            FeedState::Unsynchronized => "unsynchronized",
            FeedState::ApplyingSnapshot => "applying_snapshot",
            FeedState::Live => "live",
            FeedState::Gap => "gap",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_wire_values_are_unique() {
        let mut seen = Vec::new();
        for reason in RejectReason::ALL {
            assert!(!seen.contains(&reason.wire()), "duplicate {reason:?}");
            seen.push(reason.wire());
        }
    }

    #[test]
    fn side_wire_round_trip() {
        for side in [Side::Buy, Side::Sell] {
            assert_eq!(Side::from_wire(side.wire()), Some(side));
        }
        assert_eq!(Side::from_wire(0), None);
        assert_eq!(Side::from_wire(3), None);
    }
}
