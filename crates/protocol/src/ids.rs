//! Integer domain types. No floating point anywhere in the trading path.

macro_rules! id_type {
    ($name:ident, $inner:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(pub $inner);

        impl $name {
            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

macro_rules! counter_type {
    ($name:ident) => {
        id_type!($name, u64);

        impl $name {
            /// Returns the next value, or `None` when the counter is exhausted.
            pub fn next(self) -> Option<Self> {
                self.0.checked_add(1).map($name)
            }
        }
    };
}

id_type!(InstrumentId, u32);
id_type!(AccountId, u32);
id_type!(ClientOrderId, u64);
id_type!(RequestId, u64);

counter_type!(OrderId);
counter_type!(TradeId);
counter_type!(IngressSeq);
counter_type!(EngineSeq);
counter_type!(OutputSeq);
counter_type!(PrioritySeq);

/// Signed tick count relative to the instrument's price scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PriceTicks(pub i64);

impl PriceTicks {
    pub const fn get(self) -> i64 {
        self.0
    }

    pub fn checked_add_ticks(self, ticks: i64) -> Option<Self> {
        self.0.checked_add(ticks).map(PriceTicks)
    }

    pub fn checked_sub_ticks(self, ticks: i64) -> Option<Self> {
        self.0.checked_sub(ticks).map(PriceTicks)
    }

    /// Midpoint rounded toward negative infinity so both sides agree on the result.
    pub fn midpoint(bid: PriceTicks, ask: PriceTicks) -> Option<PriceTicks> {
        let sum = bid.0.checked_add(ask.0)?;
        Some(PriceTicks(sum.div_euclid(2)))
    }
}

impl std::fmt::Display for PriceTicks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unsigned lot count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct QuantityLots(pub u64);

impl QuantityLots {
    pub const ZERO: QuantityLots = QuantityLots(0);

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn checked_add(self, other: QuantityLots) -> Option<Self> {
        self.0.checked_add(other.0).map(QuantityLots)
    }

    pub fn checked_sub(self, other: QuantityLots) -> Option<Self> {
        self.0.checked_sub(other.0).map(QuantityLots)
    }

    /// Quantity times price, widened so realistic sizes cannot overflow.
    pub fn checked_notional(self, price: PriceTicks) -> Option<Notional> {
        let price = i128::from(price.0).unsigned_abs();
        u128::from(self.0).checked_mul(price).map(Notional)
    }
}

impl std::fmt::Display for QuantityLots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Absolute notional in tick * lot units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Notional(pub u128);

impl Notional {
    pub const ZERO: Notional = Notional(0);

    pub const fn get(self) -> u128 {
        self.0
    }

    pub fn checked_add(self, other: Notional) -> Option<Self> {
        self.0.checked_add(other.0).map(Notional)
    }

    pub fn checked_sub(self, other: Notional) -> Option<Self> {
        self.0.checked_sub(other.0).map(Notional)
    }
}

impl std::fmt::Display for Notional {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notional_uses_wide_intermediate() {
        let quantity = QuantityLots(u64::MAX);
        let price = PriceTicks(i64::MAX);
        let notional = quantity.checked_notional(price).unwrap();
        assert_eq!(notional.get(), u128::from(u64::MAX) * (i64::MAX as u128));
    }

    #[test]
    fn midpoint_rounds_down_for_negative_prices() {
        let mid = PriceTicks::midpoint(PriceTicks(-3), PriceTicks(-2)).unwrap();
        assert_eq!(mid, PriceTicks(-3));
    }

    #[test]
    fn counters_report_exhaustion() {
        assert_eq!(OrderId(u64::MAX).next(), None);
        assert_eq!(OrderId(7).next(), Some(OrderId(8)));
    }
}
