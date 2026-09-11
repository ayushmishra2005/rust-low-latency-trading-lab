//! Canonical little-endian encoding of output events.
//!
//! The same bytes feed the output digest and the durable journal, so replay
//! comparisons and recovery always agree. Nothing here depends on memory layout.

use crate::enums::{FeedState, OrderState, OrderType, RejectReason, ReportKind, Side};
use crate::error::DecodeError;
use crate::events::{EngineStateEvent, ExecutionReport, OutputEvent, StateEvent, TradeEvent};
use crate::ids::{
    AccountId, ClientOrderId, EngineSeq, InstrumentId, OrderId, OutputSeq, PriceTicks,
    QuantityLots, RequestId, TradeId,
};
use crate::read::Reader;

const TAG_TRADE: u8 = 1;
const TAG_REPORT: u8 = 2;
const TAG_STATE: u8 = 3;

pub fn encode_output(event: &OutputEvent, out: &mut Vec<u8>) {
    match event {
        OutputEvent::Trade(trade) => {
            out.push(TAG_TRADE);
            out.extend_from_slice(&trade.output_seq.0.to_le_bytes());
            out.extend_from_slice(&trade.engine_seq.0.to_le_bytes());
            out.extend_from_slice(&trade.engine_time_ns.to_le_bytes());
            out.extend_from_slice(&trade.trade_id.0.to_le_bytes());
            out.extend_from_slice(&trade.instrument.0.to_le_bytes());
            out.extend_from_slice(&trade.maker_order_id.0.to_le_bytes());
            out.extend_from_slice(&trade.taker_order_id.0.to_le_bytes());
            out.extend_from_slice(&trade.maker_account.0.to_le_bytes());
            out.extend_from_slice(&trade.taker_account.0.to_le_bytes());
            out.push(trade.aggressor.wire());
            out.extend_from_slice(&trade.price.0.to_le_bytes());
            out.extend_from_slice(&trade.quantity.0.to_le_bytes());
        }
        OutputEvent::Report(report) => {
            out.push(TAG_REPORT);
            out.extend_from_slice(&report.output_seq.0.to_le_bytes());
            out.extend_from_slice(&report.engine_seq.0.to_le_bytes());
            out.extend_from_slice(&report.engine_time_ns.to_le_bytes());
            out.extend_from_slice(&report.account.0.to_le_bytes());
            out.extend_from_slice(&report.instrument.0.to_le_bytes());
            out.extend_from_slice(&report.request_id.0.to_le_bytes());
            out.extend_from_slice(&report.client_order_id.0.to_le_bytes());
            out.extend_from_slice(&report.order_id.0.to_le_bytes());
            out.push(report.kind.wire());
            out.push(report.state.wire());
            out.push(report.side.wire());
            out.push(report.order_type.wire());
            out.extend_from_slice(&report.price.0.to_le_bytes());
            out.extend_from_slice(&report.total_quantity.0.to_le_bytes());
            out.extend_from_slice(&report.cumulative_filled.0.to_le_bytes());
            out.extend_from_slice(&report.remaining.0.to_le_bytes());
            out.extend_from_slice(&report.last_fill_quantity.0.to_le_bytes());
            out.extend_from_slice(&report.last_fill_price.0.to_le_bytes());
            out.push(report.reject_reason.map_or(0, RejectReason::wire));
        }
        OutputEvent::State(state) => {
            out.push(TAG_STATE);
            out.extend_from_slice(&state.output_seq.0.to_le_bytes());
            out.extend_from_slice(&state.engine_seq.0.to_le_bytes());
            out.extend_from_slice(&state.engine_time_ns.to_le_bytes());
            match state.event {
                EngineStateEvent::KillSwitchEngaged => out.push(1),
                EngineStateEvent::KillSwitchReleased => out.push(2),
                EngineStateEvent::AccountEnabledChanged { account, enabled } => {
                    out.push(3);
                    out.extend_from_slice(&account.0.to_le_bytes());
                    out.push(u8::from(enabled));
                }
                EngineStateEvent::AccountLimitsChanged { account } => {
                    out.push(4);
                    out.extend_from_slice(&account.0.to_le_bytes());
                }
                EngineStateEvent::FeedStateChanged { instrument, state } => {
                    out.push(5);
                    out.extend_from_slice(&instrument.0.to_le_bytes());
                    out.push(state.wire());
                }
            }
        }
    }
}

/// Decodes one canonical output event and returns the bytes consumed.
pub fn decode_output(bytes: &[u8]) -> Result<(OutputEvent, usize), DecodeError> {
    let mut reader = Reader::new(bytes);
    let event = match reader.u8()? {
        TAG_TRADE => OutputEvent::Trade(TradeEvent {
            output_seq: OutputSeq(reader.u64()?),
            engine_seq: EngineSeq(reader.u64()?),
            engine_time_ns: reader.u64()?,
            trade_id: TradeId(reader.u64()?),
            instrument: InstrumentId(reader.u32()?),
            maker_order_id: OrderId(reader.u64()?),
            taker_order_id: OrderId(reader.u64()?),
            maker_account: AccountId(reader.u32()?),
            taker_account: AccountId(reader.u32()?),
            aggressor: read_side(&mut reader)?,
            price: PriceTicks(reader.i64()?),
            quantity: QuantityLots(reader.u64()?),
        }),
        TAG_REPORT => OutputEvent::Report(ExecutionReport {
            output_seq: OutputSeq(reader.u64()?),
            engine_seq: EngineSeq(reader.u64()?),
            engine_time_ns: reader.u64()?,
            account: AccountId(reader.u32()?),
            instrument: InstrumentId(reader.u32()?),
            request_id: RequestId(reader.u64()?),
            client_order_id: ClientOrderId(reader.u64()?),
            order_id: OrderId(reader.u64()?),
            kind: read_report_kind(&mut reader)?,
            state: read_order_state(&mut reader)?,
            side: read_side(&mut reader)?,
            order_type: OrderType::from_wire(reader.u8()?).ok_or_else(|| reader.invalid_field())?,
            price: PriceTicks(reader.i64()?),
            total_quantity: QuantityLots(reader.u64()?),
            cumulative_filled: QuantityLots(reader.u64()?),
            remaining: QuantityLots(reader.u64()?),
            last_fill_quantity: QuantityLots(reader.u64()?),
            last_fill_price: PriceTicks(reader.i64()?),
            reject_reason: read_reject(&mut reader)?,
        }),
        TAG_STATE => {
            let output_seq = OutputSeq(reader.u64()?);
            let engine_seq = EngineSeq(reader.u64()?);
            let engine_time_ns = reader.u64()?;
            let event = match reader.u8()? {
                1 => EngineStateEvent::KillSwitchEngaged,
                2 => EngineStateEvent::KillSwitchReleased,
                3 => EngineStateEvent::AccountEnabledChanged {
                    account: AccountId(reader.u32()?),
                    enabled: match reader.u8()? {
                        0 => false,
                        1 => true,
                        _ => return Err(reader.invalid_field()),
                    },
                },
                4 => EngineStateEvent::AccountLimitsChanged {
                    account: AccountId(reader.u32()?),
                },
                5 => EngineStateEvent::FeedStateChanged {
                    instrument: InstrumentId(reader.u32()?),
                    state: read_feed_state(&mut reader)?,
                },
                _ => return Err(reader.invalid_field()),
            };
            OutputEvent::State(StateEvent {
                output_seq,
                engine_seq,
                engine_time_ns,
                event,
            })
        }
        _ => return Err(reader.invalid_field()),
    };
    Ok((event, reader.offset))
}

fn read_side(reader: &mut Reader<'_>) -> Result<Side, DecodeError> {
    Side::from_wire(reader.u8()?).ok_or_else(|| reader.invalid_field())
}

fn read_report_kind(reader: &mut Reader<'_>) -> Result<ReportKind, DecodeError> {
    match reader.u8()? {
        1 => Ok(ReportKind::Accepted),
        2 => Ok(ReportKind::Rejected),
        3 => Ok(ReportKind::PartiallyFilled),
        4 => Ok(ReportKind::Filled),
        5 => Ok(ReportKind::Cancelled),
        6 => Ok(ReportKind::Replaced),
        _ => Err(reader.invalid_field()),
    }
}

fn read_order_state(reader: &mut Reader<'_>) -> Result<OrderState, DecodeError> {
    match reader.u8()? {
        1 => Ok(OrderState::Working),
        2 => Ok(OrderState::PartiallyFilled),
        3 => Ok(OrderState::Filled),
        4 => Ok(OrderState::Cancelled),
        _ => Err(reader.invalid_field()),
    }
}

fn read_feed_state(reader: &mut Reader<'_>) -> Result<FeedState, DecodeError> {
    match reader.u8()? {
        1 => Ok(FeedState::Unsynchronized),
        2 => Ok(FeedState::ApplyingSnapshot),
        3 => Ok(FeedState::Live),
        4 => Ok(FeedState::Gap),
        _ => Err(reader.invalid_field()),
    }
}

fn read_reject(reader: &mut Reader<'_>) -> Result<Option<RejectReason>, DecodeError> {
    let value = reader.u8()?;
    if value == 0 {
        return Ok(None);
    }
    RejectReason::ALL
        .into_iter()
        .find(|reason| reason.wire() == value)
        .map(Some)
        .ok_or_else(|| reader.invalid_field())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_events_round_trip() {
        let events = [
            OutputEvent::Trade(TradeEvent {
                output_seq: OutputSeq(1),
                engine_seq: EngineSeq(2),
                engine_time_ns: 3,
                trade_id: TradeId(4),
                instrument: InstrumentId(5),
                maker_order_id: OrderId(6),
                taker_order_id: OrderId(7),
                maker_account: AccountId(8),
                taker_account: AccountId(9),
                aggressor: Side::Sell,
                price: PriceTicks(-10),
                quantity: QuantityLots(11),
            }),
            OutputEvent::Report(ExecutionReport {
                output_seq: OutputSeq(12),
                engine_seq: EngineSeq(13),
                engine_time_ns: 14,
                account: AccountId(15),
                instrument: InstrumentId(16),
                request_id: RequestId(17),
                client_order_id: ClientOrderId(18),
                order_id: OrderId(19),
                kind: ReportKind::Rejected,
                state: OrderState::Cancelled,
                side: Side::Buy,
                order_type: OrderType::Market,
                price: PriceTicks(20),
                total_quantity: QuantityLots(21),
                cumulative_filled: QuantityLots(22),
                remaining: QuantityLots(23),
                last_fill_quantity: QuantityLots(24),
                last_fill_price: PriceTicks(25),
                reject_reason: Some(RejectReason::PriceCollar),
            }),
            OutputEvent::State(StateEvent {
                output_seq: OutputSeq(26),
                engine_seq: EngineSeq(27),
                engine_time_ns: 28,
                event: EngineStateEvent::FeedStateChanged {
                    instrument: InstrumentId(29),
                    state: FeedState::Gap,
                },
            }),
        ];

        for event in events {
            let mut bytes = Vec::new();
            encode_output(&event, &mut bytes);
            let (decoded, consumed) = decode_output(&bytes).unwrap();
            assert_eq!(consumed, bytes.len());
            assert_eq!(decoded, event);
        }
    }

    #[test]
    fn truncated_output_does_not_panic() {
        let event = OutputEvent::State(StateEvent {
            output_seq: OutputSeq(1),
            engine_seq: EngineSeq(1),
            engine_time_ns: 1,
            event: EngineStateEvent::KillSwitchEngaged,
        });
        let mut bytes = Vec::new();
        encode_output(&event, &mut bytes);
        for len in 0..bytes.len() {
            assert!(decode_output(&bytes[..len]).is_err());
        }
    }
}
