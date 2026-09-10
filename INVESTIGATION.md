# Rust Low-Latency Trading Lab Investigation

## 1. Goals

This repository should become an open-source engineering lab for a deterministic, latency-sensitive electronic trading system. Its purpose is to demonstrate sound Rust, systems programming, concurrency, financial state transitions, measurement discipline, testing, and recoverable integration boundaries. It must be credible as a senior engineering portfolio project without representing itself as a real exchange, a deployable HFT platform, or evidence of production trading experience.

The project has exactly two implementation phases:

- **Phase 1 — Low-Latency Trading Core:** a compact binary market-data feed, a reference market view, an in-memory matching book, deterministic execution, pre-trade risk, a dedicated-thread runtime, replay, tests, and benchmarks.
- **Phase 2 — Control, Observability and Digital-Asset Settlement:** a TypeScript operational API, telemetry, durable settlement coordination, a Solana adapter, and an optional Canton adapter. Every slow or failure-prone external dependency remains outside the execution hot path.

The design should make the following claims demonstrable through code and evidence:

- Identical recorded input produces identical decisions, output ordering, final state, and checksum.
- Mutable trading state has one owner and does not require a mutex in the hot path.
- Price, quantity, and notional calculations use exact integer representations and checked arithmetic.
- Queue capacities, overflow behavior, risk rules, and order-state transitions are explicit.
- Benchmarks report observed results, distributions, and their environment rather than estimates.
- External settlement is idempotent and reconcilable but never retroactively changes an executed trade.

The first implementation should favor a clear safe-Rust baseline. Complexity is justified only by correctness, bounded resource use, or a measured performance problem.

## 2. Non-Goals

- This is not a production exchange, broker, clearing system, custody system, or HFT strategy.
- It will not claim regulatory compliance, fault tolerance, losslessness, fairness, determinism across unrecorded live races, or any latency figure that has not been measured.
- It will not initially connect directly to NASDAQ, CME, or a paid market-data vendor.
- It will not reconstruct a full order book from trade-only public data.
- It will not implement every exchange order type. Phase 1 needs market and limit orders, cancel, and replace; auctions, pegged orders, stop orders, hidden quantity, and complex time-in-force policies are out of scope.
- It will not use floating-point values for prices, quantities, positions, limits, balances, or settlement amounts.
- It will not put Node.js, REST, WebSockets, Prometheus exposition, disk I/O, Solana RPC, a Canton participant, or a database call in the execution path.
- It will not promise zero allocation everywhere. The target is no allocation in the steady-state measured hot path where practical, with allocations at initialization and cold boundaries permitted.
- It will not introduce unsafe Rust, a custom allocator, a custom lock-free queue, kernel bypass, SIMD, busy polling, or a direct-indexed price ladder merely to appear sophisticated.
- It will not add a third implementation phase. Possible later optimizations remain measurement-gated alternatives inside one of the two stated phases.

## 3. HFT / Low-Latency Concepts We Need to Demonstrate

### Low latency and high throughput are different objectives

**Low latency** is the elapsed time for one event or order to traverse a defined path. The important properties are the full distribution, especially p99, p99.9, and maximum, plus jitter and outliers. A design can have excellent average latency but unacceptable tails.

**High throughput** is the number of messages, orders, or trades completed per unit of time under a stated workload. Batching often improves throughput by amortizing synchronization, parsing, and system-call costs, but a batch also waits to fill and can increase individual-event latency. The project must measure both rather than substitute one for the other.

The following concepts should be visible in the architecture and experiments:

| Concept | What the project should demonstrate | What must not be assumed |
|---|---|---|
| Tail latency | Percentiles and maximum from an explicit start/end boundary | The mean represents user experience |
| Jitter | Run-to-run variance, scheduler interruptions, page faults, frequency and thermal effects | Rust removes operating-system or hardware noise |
| Cache locality | Compact nodes, integer identifiers, single-owner state, preallocation | A flat array is always faster than a tree |
| Allocation | Allocation counting and warmed steady-state checks | “Zero allocation” without observing the allocator |
| Context switches | Dedicated threads and optional affinity experiments | Pinning always improves performance |
| Locks | No shared mutable book/risk state; queues at ownership boundaries | “Lock-free” always means lower latency |
| Atomics | Restricted to queue coordination, health publication, and the emergency kill latch | Atomics are free or automatically correct |
| Syscalls | No file, socket, logging, or metrics exposition calls from the engine loop | Async I/O makes CPU work faster |
| Batching | Separate throughput-oriented configurations | Larger batches are harmless to tail latency |
| Backpressure | Bounded queues and explicit full-queue policies | Dropping data is acceptable because a queue is full |
| Replay | A recorded total order and logical clock | A live multi-source arrival order is reproducible before it is recorded |

### Required for correctness, useful for performance, and premature optimization

**Required for correctness:** exact arithmetic, protocol bounds checks, sequence validation, deterministic tie-breaking, explicit state transitions, unique IDs, reservation-aware risk, single-owner mutation, bounded queues, idempotent request handling, canonical replay hashing, and recoverable settlement state machines.

**Useful for performance from the start:** dedicated long-lived threads, SPSC topology, preallocated order storage and output buffers, compact numeric messages, no hot-path strings, a sparse ordered price map, O(1) indexed cancellation, and off-thread serialization/telemetry.

**Premature until measured:** unsafe parsing, a hand-built lock-free ring, a custom allocator, direct-indexed ladders, huge pages, kernel bypass, SIMD decoding, profile-guided optimization, aggressive LTO tuning, NUMA placement, or replacing every branch with a clever abstraction.

Every optimization should preserve a simple reference model and deterministic corpus so performance changes can be checked against the same behavior.

## 4. Phase 1 Architecture

### Recommended logical pipeline

```text
generated or recorded bytes
          |
          v
feed I/O + framing + validation thread
          |
     bounded SPSC
          |
          v
single-owner engine thread
  1. assign/verify engine sequence
  2. update external MarketView
  3. obtain deterministic order request, if any
  4. validate request and pre-trade risk
  5. match against the simulated venue OrderBook
  6. update positions and risk reservations
  7. emit ordered reports/trades
          |
     bounded SPSC
          |
          v
output fan-out / recorder thread
  - binary event journal
  - assertions/checkpoints
  - cold telemetry
```

The most important modeling decision is to keep two books distinct:

- `MarketView` represents external market data used for reference prices, staleness, and strategy stimuli. Snapshot and incremental feed events update it.
- `OrderBook` represents orders accepted by this lab's matching engine. New, cancel, replace, market, and limit requests mutate it and produce trades.

Mixing external price-level updates with locally accepted orders would make ownership, matching semantics, and replay ambiguous. Public trade data may stimulate the simulator, but trade-only files cannot reconstruct the external `MarketView`.

### Core components

1. **Protocol model:** compact integer domain types, normalized input events, output events, reject codes, and explicit schema versions.
2. **Market-data codec:** framing, decoding, validation, source sequence tracking, snapshots, gap handling, deterministic generation, and file replay.
3. **Trading core:** a pure state machine owning market views, matching books, risk state, positions, request deduplication, and ID counters.
4. **Runtime:** dedicated threads, SPSC queues, wait strategies, shutdown, affinity options, and output draining.
5. **Simulator/replayer:** deterministic scenarios, seeded generation, recorded input, checksum comparison, and benchmark workloads.

The trading core should contain no socket, filesystem, wall-clock, async-runtime, logging-subscriber, blockchain, or Node.js dependency. Its public operation is conceptually `apply(input, logical_time, reusable_output_buffer)`. The actual API should remain concrete and small; a framework of traits is not needed.

### Event ordering boundary

Phase 1 should start with one normalized input stream. Generated market events and generated order requests are merged before the inbound SPSC queue and assigned an `IngressSeq`. The engine processes one event at a time and never selects between two inbound queues. That simple total order is essential for reproducibility.

Phase 2 control commands can arrive on a separate cold queue. The engine assigns their definitive `EngineSeq` when it observes them and records the resulting interleaving. A live run is reproducible only after this merged order is recorded.

## 5. Market Data Design

### Best starting approach

Start with a **deterministic generator plus a project-owned compact binary feed format**. This is the only starting option that naturally exercises snapshots, incrementals, gaps, duplicates, malformed frames, timestamp anomalies, and controlled workload mixes without a paid source or a third-party schema dependency.

Add an optional offline converter for recorded public crypto data after the codec and replay path work. Binance publishes daily and monthly public market-data files with checksums,[^1] and Coinbase documents a level-2 snapshot/update workflow.[^2] These are useful realism inputs, but the repository must normalize them into its own recorded format and store source/license metadata. A small committed fixture is preferable to making tests depend on the network or a large external archive.

Recommended progression:

1. Seeded generator producing valid level-2 snapshots, absolute level updates, trades, heartbeats, and order requests.
2. Golden binary fixtures covering each frame type and malformed case.
3. Recorder/replayer for the normalized stream.
4. Optional Coinbase level-2 capture or Binance public-trade converter. Trades remain trade stimuli unless paired with genuine depth snapshots and updates.

### Binary framing

Use a deliberately small manually specified format, not the in-memory Rust representation. Native Rust layout, enum discriminants, padding, pointer width, and hash-map state are not a stable wire contract.

Suggested file header:

| Field | Purpose |
|---|---|
| Magic bytes | Reject the wrong file type immediately |
| Format major/minor | Explicit compatibility decision |
| Endianness marker | The first version should mandate little-endian |
| Header length | Permit future header fields |
| Run/engine instance ID | Stable identity for IDs and replay |
| Generator seed or source metadata | Reproduce generation or identify capture |
| Instrument table | Numeric symbol ID, tick size, lot size, base/quote scales |
| Start wall time | Informational UTC metadata, excluded from decisions |
| Header checksum | Detect accidental corruption |

Suggested frame envelope:

| Field | Type | Rule |
|---|---|---|
| Frame length | `u32` | Includes a fixed header and payload; bounded before allocation |
| Message type | `u8` | Unknown types rejected for v1 unless explicitly skippable |
| Flags | `u8` | Reserved bits must be zero |
| Schema version | `u16` | Exact decoder selection |
| Source sequence | `u64` | Per feed/channel; zero only for documented non-sequenced messages |
| Source timestamp | `u64` ns | Informational, never trusted for ordering |
| Recorded receive time | `u64` ns | Monotonic elapsed time from run epoch |
| Instrument ID | `u32` | Must exist in the header instrument table |
| Payload | fixed by type | Integers only, exact widths, explicit signedness |
| CRC32 | `u32` | File integrity and corruption detection, not authentication |

The implementation should use checked slice reads and `from_le_bytes` initially. `zerocopy` provides safe layout-aware conversions and byte-order numeric types,[^3] but it should be evaluated only if decoding appears materially in profiles. JSON is appropriate for the Phase 2 control boundary, not for the hot feed. FlatBuffers, Cap'n Proto, Protobuf, or schema-generated codecs offer evolution features but add code generation and more machinery than this deliberately small protocol needs.

`bincode` should not be selected for a new persistent format: its current release explicitly states that the project is unmaintained.[^4] `postcard` has a documented stable wire format and can encode into caller-provided storage,[^5] so it is a credible alternative for cold internal records, but a small manual market-data schema makes widths, bounds, and compatibility more visible for this lab.

### Message set

Keep the v1 set small:

- `SnapshotBegin { snapshot_seq }`
- `SnapshotLevel { side, price_ticks, quantity_lots }`
- `SnapshotEnd { snapshot_seq, level_count, state_crc }`
- `LevelSet { side, price_ticks, new_quantity_lots }`; zero quantity deletes a level
- `MarketTrade { aggressor_side, price_ticks, quantity_lots }`
- `Heartbeat`
- `FeedReset { new_epoch }`
- normalized `OrderRequest` messages for deterministic merged replay

Absolute `LevelSet` quantities are preferable to deltas in the first format. Reapplying an absolute update is easier to reason about, while duplicates are still detected by sequence. If order-level external data is added later, it should be a separate schema rather than overloading level updates.

### Sequence and synchronization state machine

Each channel has `Unsynchronized`, `ApplyingSnapshot`, `Live`, and `Gap` states.

- Startup begins `Unsynchronized`; strategy-generated new orders that require reference data are rejected.
- A complete validated snapshot is built in scratch state and becomes visible atomically only at `SnapshotEnd`.
- In `Live`, the next incremental must have exactly `last_source_seq + 1`.
- A repeated sequence is recorded as a duplicate and ignored only under an explicit duplicate policy. A lower non-duplicate sequence is an out-of-order error.
- A forward jump enters `Gap`; the incomplete state is not treated as current.
- A live adapter may buffer a bounded number of incrementals while fetching a snapshot, then discard entries at or below the snapshot sequence and apply the contiguous remainder, matching the documented snapshot-plus-queued-update pattern used by real feeds.[^2]
- If the buffer overflows or continuity cannot be proven, discard it and request another snapshot.

The generator should deliberately inject gaps, duplicates, truncation, invalid types, oversized lengths, bad CRCs, zero/overflowing quantities, invalid sides, unknown instruments, and interrupted snapshots.

### Malformed-message policy

The decoder must never panic on untrusted bytes and must never allocate based on an unchecked length. It returns a compact error containing category, source sequence if available, and byte offset.

- **Replay strict mode:** stop at the first malformed frame and report its offset. Silently skipping would create a misleading checksum.
- **Live/capture mode:** mark the channel unhealthy, count the error, stop applying incrementals, and require a new snapshot. Do not log the entire untrusted payload.
- **Fuzz mode:** any byte string may be supplied; success must imply a fully validated message, and failure must be bounded and panic-free.

### Timestamp rules

Use three distinct notions of time:

- `source_time_ns`: supplied by the data source and retained for analysis; it may jump or arrive out of order.
- `receive_time_ns`: monotonic elapsed time stamped at ingress and recorded.
- `engine_time_ns`: the logical time passed to the deterministic engine, normally derived from recorded receive time.

The core must not call `SystemTime::now()` or `Instant::now()` while making a decision. Live ingress records time; replay supplies the same value. Wall-clock UTC belongs in file metadata and logs only.

## 6. Order Book Design

### Semantics

The simulated venue book supports bids and asks across multiple price levels with strict price-time priority:

- Higher bid prices rank first; lower ask prices rank first.
- Within a price, the lowest priority sequence ranks first.
- An incoming crossing order is the aggressor; resting orders are makers.
- Trades occur at the resting order's price.
- A market order consumes available liquidity up to its protection price and cancels any remainder.
- A crossing limit order consumes eligible prices, then rests any remainder at its limit.
- A fill may be partial or complete for either side.

Prices are signed or unsigned fixed-width **tick counts** only after validating the instrument's allowed range. Quantities are unsigned **lot counts**. Notional uses a wider checked intermediate, normally `u128`, before comparison to configured bounds. Decimal strings are parsed and tick/lot aligned outside the matching loop.

### Data-structure alternatives

| Alternative | Advantages | Costs and risks | Decision |
|---|---|---|---|
| `BTreeMap<Price, PriceLevel>` | Standard library, sparse prices, ordered traversal, direct best-level access, clear `O(log L)` behavior | Tree nodes allocate as new levels appear; pointer chasing and branch behavior may affect tails | **Initial price-level index** |
| Direct-indexed price array | `O(1)` level lookup, compact traversal within a bounded band | Requires a known range, can waste memory, and needs rebasing/window logic | Benchmark later for tightly bounded instruments |
| Sorted `Vec<PriceLevel>` | Compact and cache-friendly reads | Insert/delete shifts are `O(L)` under price churn | Useful for immutable snapshots, not the active matching book |
| Hash map plus best-price heap | Fast average lookup | Stale heap entries, two structures, more invariants, and no simple ordered traversal | Reject initially |
| Tree node per order | Straightforward ordering | Excess allocation and weak locality; cancel lookup still needed | Reject |
| `VecDeque<OrderId>` per level | Simple FIFO | Arbitrary cancel/replace is `O(n)` or needs tombstones; tombstones create unbounded cleanup spikes | Reference model only |
| Safe indexed intrusive list | `O(1)` unlink, FIFO order, stable numeric links, preallocatable | More link invariants than `VecDeque` | **Initial per-level queue** |
| Pointer-based intrusive list | Direct node links | Unsafe lifetime/aliasing burden without demonstrated benefit | Do not use initially |

Rust's `BTreeMap` is an ordered B-tree map,[^6] which fits a sparse price domain and gives a straightforward baseline. The recommended active book is:

- one `BTreeMap<PriceTicks, PriceLevel>` per side;
- `PriceLevel { head_slot, tail_slot, order_count, total_remaining }`;
- a preallocated `slab::Slab<OrderNode>` containing safe integer `prev`/`next` slot indexes;
- `HashMap<OrderId, SlotIndex>` for direct cancel/replace lookup;
- each `OrderNode` stores account, side, price, remaining, cumulative filled, priority sequence, and current total quantity.

`slab` is backed by a vector and supports preallocation, but it will grow when full and reuses keys.[^7] Therefore the engine must enforce `max_live_orders`, initialize the slab to that capacity, reject before insertion when it is full, and never expose a slot index as an external identity. `OrderId` remains the external key, so slot reuse cannot revive an old order.

This design introduces one modest custom invariant—an indexed doubly linked FIFO—because it avoids both linear cancellation and unsafe pointers. It is sophisticated only where the required operations justify it.

### Operation behavior

- **New limit:** validate and reserve risk first; match eligible levels; if remaining, allocate one slot and append it to the tail of its level.
- **New market:** require a current reference and a deterministic protection price; match eligible levels; never rest the remainder.
- **Cancel:** find the slot by `OrderId`, unlink it in `O(1)`, release remaining risk reservation, remove an empty price level, and emit the terminal report.
- **Replace:** validate the whole replacement and risk delta before mutation. A same-price quantity decrease keeps priority. A price change or quantity increase loses priority and receives a new priority sequence. If validation fails, the original order remains unchanged.
- **Partial fill:** decrement maker and aggressor remaining quantities, increment cumulative filled, update the level aggregate, risk reservation, positions, and reports in one deterministic order.
- **Full fill:** unlink and remove the maker before moving to the next order; delete an empty price level.

Define replace quantity as a **new total order quantity**, not a new remaining quantity. It must be at least cumulative filled; new remaining equals new total minus cumulative filled. A value equal to cumulative filled is an explicit cancel of the remainder. This definition makes reports auditable and avoids ambiguity.

### Allocation caveat

The slab and lookup table can be preallocated. A `BTreeMap` may still allocate when a previously unseen price level is inserted. That is acceptable for the initial safe baseline and must be disclosed. If allocation profiling shows price-level churn dominates meaningful tail latency, compare these measured alternatives:

1. retain and recycle empty `PriceLevel` objects;
2. a pooled level store plus an ordered index;
3. a direct-indexed ladder over a configured collar band.

Do not adopt an array ladder before the supported price range and memory cost are known.

## 7. Matching and Execution Model

### Execution path

```text
MarketEvent
  -> validate feed sequence and update MarketView
  -> deterministic simulator/strategy decision, if configured
  -> OrderRequest
  -> request idempotency and field validation
  -> PreTradeRisk
  -> MatchingBook
  -> TradeEvent + ExecutionReport(s)
  -> position/reservation update
  -> ordered output batch
```

For direct order-request inputs, the path starts at request validation. The project need not build a profitable strategy. A small deterministic strategy fixture may translate specific market events into requests solely to exercise the full causal path.

### IDs

Use numeric IDs in the hot path and render them as text only at external boundaries.

| ID | Scope and generation |
|---|---|
| `RunId` / `EngineInstanceId` | 128-bit value stored in the log header; replay reuses it |
| `InstrumentId` | `u32`, assigned in the instrument table |
| `AccountId` | `u32` or `u64`, configured before the run |
| `ClientOrderId` | `u64`, unique within account and engine instance |
| `RequestId` | `u64`, unique/monotonic within an account session; covers new/cancel/replace |
| `OrderId` | engine-assigned monotonic `u64`; never a slab index |
| `TradeId` | engine-assigned monotonic `u64`, paired with engine instance externally |
| `IngressSeq` | total order assigned before the inbound queue |
| `EngineSeq` | increments once for every applied input or recorded control event |
| `OutputSeq` | increments for every execution report, trade, reject, or state event |
| `PrioritySeq` | increments whenever an order obtains or loses queue priority |

All counters must check exhaustion. Wrapping an ID is a fatal engine condition, not a valid rollover.

### Order states and reports

Persisted accepted-order states:

- `Working`
- `PartiallyFilled`
- `Filled` (terminal)
- `Cancelled` (terminal)

`Rejected` is a request outcome, not a live order. `PendingNew` is unnecessary because acceptance and book mutation are synchronous on one engine thread. Replace is a report/action; the accepted order remains `Working` or `PartiallyFilled` with a new revision and possibly a new priority sequence.

Execution reports should include:

- output and engine sequence;
- request, client order, and engine order IDs where applicable;
- account and instrument IDs;
- action/result and order state;
- side, type, price, total quantity, last fill quantity/price, cumulative fill, remaining;
- reject reason or terminal reason;
- logical engine timestamp.

A `TradeEvent` includes trade ID, engine/output sequence, instrument, maker/taker order and account IDs, price, quantity, aggressor side, and logical time. Emit the trade and both order reports in one documented order, for example: trade, maker report, taker report. Keep that order stable.

### Reject reasons

Use a closed numeric enum, with at least:

- malformed or unsupported request;
- unknown instrument/account/order;
- invalid side/type/price/quantity;
- price or quantity not aligned to tick/lot;
- duplicate request conflict;
- stale or out-of-order client sequence;
- duplicate client order ID;
- account disabled;
- global kill active;
- market data unsynchronized or stale;
- max order quantity/notional;
- price collar;
- max position/gross exposure;
- capacity exhausted;
- arithmetic overflow;
- order already terminal;
- invalid replace quantity/state.

Reject checks run in a fixed order so one invalid request always produces the same primary reason. Exact duplicate requests return the recorded prior result; the same `RequestId` with different content returns `DuplicateRequestConflict` and has no effect.

### Deterministic execution rules

- Only `IngressSeq`/`EngineSeq` and explicit price-time rules decide order; source or wall-clock time never breaks ties.
- The engine processes one input atomically before the next.
- Hash-map iteration order is never used to choose an order, assign an ID, emit a report, or construct a checksum.
- The resting price determines execution price.
- A market order has a deterministic protection bound; “unbounded market price” is not allowed.
- Output order is fixed, including maker/taker report order.
- Replace validation is two-phase; a rejected replace cannot partially alter the book or reservation.
- The strategy fixture, if enabled, consumes no random source except a seed stored in the log.

Self-trade prevention is a realistic but venue-specific policy. The initial decision should be either explicitly “allowed in this lab” or a single simple mode such as cancel-aggressor. It must not be left implicit, because it changes trades, risk, and replay.

## 8. Risk Engine

### Ownership and hot-path model

Risk state belongs to the same engine thread as the matching book. It includes account status, limits, positions, working-order reservations, last accepted client sequence, deduplication results, reference-price state, and the global kill state. No mutex, database, RPC, or configuration-file read occurs while checking an order.

### Fixed validation order

1. Detect exact duplicate/retry and return its prior result; reject a conflicting reuse.
2. Validate request schema, account, instrument, order state, integer ranges, tick, and lot.
3. Check global kill and account enabled state. Cancels remain allowed.
4. Validate client request sequence.
5. Require synchronized, sufficiently fresh reference data when the rule or order type needs it.
6. Check maximum order quantity.
7. Compute conservative price and checked notional; apply price collar and maximum order notional.
8. Compute projected working reservations, position, and gross exposure.
9. Commit reservation and matching only after all checks pass.

This order is part of the protocol. Changing it can change reject reasons and replay output.

### Controls

| Control | Initial rule | Hot-path representation |
|---|---|---|
| Max order quantity | `new_remaining` or market requested quantity must be within per-account/instrument limit | integer comparison |
| Max order notional | quantity times conservative price must be within limit using checked `u128` intermediate | integer multiply + comparison |
| Max position | Worst-case long is position + open buys + new buy; worst-case short is position - open sells - new sell | running per-side reservations |
| Max gross exposure | Conservative sum of marked absolute positions plus non-netted open-order exposure | running aggregate updated on market marks and fills |
| Price collar | Limit/protection price must be within configured ticks or basis points of a valid reference | precomputed integer bounds when market changes |
| Duplicate protection | Request sequence plus bounded result cache; exact retry is idempotent | last sequence + preallocated map/ring |
| Stale market data | Reject risk-increasing orders when feed not `Live` or logical age exceeds limit | integer age and feed state |
| Sequence validation | Require the documented next/monotonic account request sequence | one counter per account/session |
| Account disable | Reject new and risk-increasing replace; permit cancel | boolean in account state |
| Global kill | Reject new and risk-increasing replace for all accounts; permit cancel and queries | latched state, with emergency atomic ingress |

### Conservative exposure and reservations

Checking only filled position is insufficient because many resting orders could all fill. Maintain open buy and open sell quantities/notionals per account and instrument. A new order is checked against the relevant worst-case side without assuming opposing working orders will offset.

On a fill:

- decrease the filled quantity from the maker/taker reservation;
- update signed position for both accounts;
- update mark-based gross exposure in a documented order;
- remove residual reservation for a completed or cancelled order.

Market-order notional and collars need a price bound. Derive a protection price from the latest valid reference midpoint or same-side best plus configured collar. Reject when no valid reference exists. The matcher stops at the protection price and cancels any unfilled remainder.

Gross exposure across symbols requires a valuation price. Phase 1 should keep the rule explicit and simple: use the last accepted reference mark per instrument, reject if a required mark is absent/stale, and use checked integer scale conversion. Do not quietly treat missing marks as zero.

### Duplicate request retention

An unbounded set of every historical `RequestId` is not acceptable. Require a monotonically increasing `ClientSeq` per account session and retain a bounded window of request fingerprints and results for immediate retries. A request older than the retained window is rejected as `SequenceTooOld`, never re-executed. Cache capacity and reconnect/session semantics must be configured and recorded.

### Kill-switch ordering

Normal limit/account updates are control events applied between engine inputs and journaled with an `EngineSeq`. Emergency kill needs a faster route when a normal control queue is congested:

- the Rust control gateway latches a cache-separated `AtomicBool`;
- the engine checks it before each risk-increasing request;
- when first observed, the engine emits `KillSwitchEngaged` at the exact engine boundary;
- replay uses that recorded event rather than racing an atomic.

The memory ordering and cost of this read should be benchmarked. Limit data itself should not be mutated through atomics; complex updates remain ordered messages. The kill switch is fail-closed across control-service restart and can be cleared only by an authenticated explicit event.

## 9. Threading and Queue Model

### Recommended initial runtime

Use three long-lived OS threads:

1. **Feed/ingress thread:** reads or generates bytes, validates framing and feed sequence, stamps/records logical time, and publishes normalized inputs.
2. **Engine thread:** exclusively owns all mutable trading state and processes inputs serially.
3. **Output thread:** drains ordered output, writes the journal, calculates checkpoints, and hands cold events to telemetry.

Tokio's own documentation recommends a dedicated thread for long-lived persistent blocking work rather than `spawn_blocking`,[^8] which aligns with the engine loop. The latency-sensitive runtime should use `std::thread` and bounded SPSC queues.

### Queue alternatives

| Queue | Fit | Trade-off | Recommendation |
|---|---|---|---|
| `rtrb::RingBuffer` | Exact SPSC, fixed capacity, no post-construction allocation by the queue, non-blocking full/empty result | Specialized dependency; wait strategy remains ours | **Initial candidate** |
| `crossbeam_queue::ArrayQueue` | Bounded, mature, supports MPMC | Pays for a more general topology and shared head/tail coordination | Benchmark baseline or Phase 2 fan-in |
| `crossbeam_channel::bounded` | Convenient blocking/select/disconnect semantics | More runtime behavior than the hot SPSC path needs | Cold/control paths |
| `std::sync::mpsc::sync_channel` | Standard library and easy baseline | General synchronized channel and parking behavior | Correctness baseline, not presumed latency winner |
| Custom ring | Exact layout and policies | Unsafe memory lifecycle and atomic-ordering risk | Only after a measured library limitation |

`rtrb` documents a fixed-capacity SPSC queue whose operations return immediately and whose queue storage does not allocate after construction.[^9] `ArrayQueue` is a fixed-capacity bounded MPMC queue.[^10] These properties justify the topology choice; they do not prove which is faster on the target hardware. Benchmark queue round-trip latency, burst behavior, empty polling, saturation, and CPU use.

### Ownership and lock avoidance

- `MarketView`, matching books, risk, positions, IDs, and request cache are ordinary mutable values on the engine thread.
- Producers transfer owned compact messages. Messages contain numeric IDs and fixed fields, not shared `Arc<String>` graphs.
- Readers do not borrow the live book. The engine periodically emits versioned snapshots/read-model deltas to a cold consumer.
- A mutex is acceptable in startup configuration, test harness coordination, or a cold adapter. It is not acceptable around the live order book.
- Atomics are limited to queue implementation, health/sequence publication, shutdown, counters proven suitable, and the emergency kill latch.

### Bounded queues and backpressure

Every queue has a configured capacity, a high-water metric, and a documented full policy.

| Boundary | May drop? | Full behavior |
|---|---|---|
| Replay/simulator input | No | Producer waits using configured strategy; run records saturation |
| Live market-data input | No silent drop | Mark feed unhealthy/gapped, stop applying, and resynchronize; optionally retain raw capture elsewhere |
| Order/control request | No | Reject before ownership transfer or apply backpressure; never acknowledge an unqueued request |
| Execution/trade/audit output | No | Engine stalls before accepting the next input; raise health fault/kill if sustained |
| Best-effort telemetry | Yes | Increment dropped-telemetry count and send a later aggregate gap indication |

The non-droppable output policy can increase latency when persistence is slow. That is honest backpressure, not a reason to lose executions. Queue capacity is a shock absorber, not a substitute for adequate consumers.

### Wait strategies

- **Park/notify:** low idle CPU but incurs wakeups and scheduler jitter.
- **Busy spin:** lowest opportunity for wakeup latency but consumes a core, raises power/thermal concerns, and can harm sibling threads.
- **Adaptive:** spin for a configurable count, yield, then park; best developer default.

Make the policy a runtime option and report it in every benchmark. Busy-spin measurements require isolated hardware and should not be the default claim.

### Affinity, cache lines, and false sharing

Queue producer and consumer indices should not share a cache line. A maintained queue should handle its internal layout; for project-owned atomics, `crossbeam_utils::CachePadded` exists specifically to separate contended values.[^11] Padding wastes memory but is appropriate for a few frequently written coordination fields.

Thread affinity is optional and must fail visibly rather than silently. `core_affinity` offers a small cross-platform API,[^12] but macOS affinity is a scheduling hint rather than Linux-style hard CPU isolation. Primary performance results should eventually be collected on a documented Linux host. Compare pinned/unpinned and avoid placing hot threads on SMT siblings when the host topology is known.

### Where Tokio belongs

**Do not use Tokio for:** matching, risk checks, book mutation, deterministic replay decisions, the persistent engine loop, or SPSC polling.

**Use Tokio in Phase 2 for:** the Rust-side control socket, multiple slow control connections, graceful network shutdown, and external I/O adapters if they are implemented in Rust. Keep the runtime on separate threads/cores. Tokio explains that CPU work without `.await` can block runtime workers and suggests a separate pool for CPU-bound work.[^13]

## 10. Memory and Allocation Strategy

### Initial strategy

- Parse into compact enums with integers and fixed-size fields.
- Intern instruments/accounts at startup; pass numeric IDs in the hot path.
- Preallocate the order slab, order-ID index, account/risk tables, request result cache, inbound/outbound queues, snapshot scratch space, and reusable per-command output buffer.
- Enforce configured maximums before insertion. A “preallocated” collection that is allowed to grow under load is not bounded.
- Reuse the output `Vec` by clearing it, not dropping it. Reserve for the worst output of one allowed command based on `max_live_orders` and documented report multiplicity.
- Store order links as compact slot indexes. Use `Option<NonZeroU32>` or a sentinel only if measured and kept clear; plain `Option<usize>` is the readability baseline.
- Avoid `String`, `format!`, `Box<dyn Error>`, backtrace capture, and JSON inside the engine loop.
- Borrow input slices during decoding and copy only the validated fixed fields needed after the buffer advances.
- Do not clone book state for queries. Publish periodic summaries or cold snapshots with `as_of_engine_seq`.

### Predictability by structure

| Area | Predictable initial choice | Remaining variability |
|---|---|---|
| Orders | Capacity-enforced slab and free list | Lookup hash probes |
| Price levels | Sparse `BTreeMap` | Allocation when a new price first appears |
| Requests | Fixed numeric message | Queue wait and producer timing |
| Outputs | Reused preallocated batch | Worst-case sweep produces many reports |
| Feed parser | Bounded frame and stack/local integer reads | File/socket read size |
| Risk | Preallocated account/instrument matrices where dimensions are small | Multi-symbol gross calculation if not incrementally maintained |

Use the system allocator initially. If the steady-state hot path has no allocation, replacing the global allocator cannot improve those events and adds a supply-chain/behavior variable. Measure allocation counts first with a test counting allocator or profiler; evaluate an alternate allocator only if remaining cold or burst allocations are significant to an explicit metric.

### Unsafe Rust gate

No project-authored unsafe Rust is justified in the initial implementation. Dependencies such as queue or parsing libraries may contain audited unsafe internals behind safe APIs.

Unsafe may be considered later only when all of these conditions hold:

1. A profiler and allocation/latency experiment identifies a specific safe operation as a material bottleneck, including tail impact.
2. A safe alternative cannot remove the cost with acceptable clarity.
3. A benchmark on the target environment shows a repeatable improvement beyond noise.
4. The unsafe surface can be isolated behind a tiny safe API with written invariants.
5. Miri where applicable, fuzzing, property tests, and a safe reference implementation compare behavior.
6. The document/README reports the measured reason, not a generic “zero-copy” claim.

Potential candidates are bounds-check-elided fixed-frame parsing or a custom ring buffer. Pointer-linked order storage is not a candidate unless indexed storage itself is proven limiting.

## 11. Deterministic Replay

### What must be recorded

Replay needs the exact merged causal input, not only market data:

- instrument/account configuration and risk limits;
- engine/run ID and all initial counters;
- normalized market snapshots and incrementals;
- new/cancel/replace requests and client sequences;
- control changes that affect decisions, including account disable and kill;
- logical receive/engine time used for staleness;
- generator seed and version, if generated;
- protocol and application version/commit metadata.

External publish time, log formatting time, thread IDs, addresses, and Prometheus scrape timing do not affect decisions and should not enter the state hash.

### Replay modes

1. **Functional replay:** no pacing; apply events as fast as possible while reusing recorded logical time.
2. **Paced replay:** reproduce recorded inter-arrival deltas for demos and queue behavior.
3. **Stress replay:** apply a controlled rate/burst transformation, clearly marked as a new benchmark workload; decisions should remain identical if only pacing changes.
4. **Step replay:** stop at an engine sequence, inspect state, and continue for debugging.

### Canonical state checksum

Never hash raw structs, memory addresses, padding, `HashMap` iteration, or native-endian bytes. Feed a versioned canonical serialization into BLAKE3; the official implementation supports streaming hashing and multiple CPU implementations,[^14] but hash speed is not hot-path critical here.

Canonical order should include every field that can affect a future decision:

1. checksum schema version, engine instance, and next ID/sequence counters;
2. instruments sorted by numeric ID;
3. feed epoch/synchronization/last sequence and market levels in defined price order;
4. matching bids and asks in defined price order, with orders in FIFO priority order;
5. each live order's full state and revision;
6. accounts sorted by ID, including enable state, limits, positions, reservations, last client sequence, and retained dedup window;
7. global kill state and current logical time.

Maintain three comparisons:

- digest of normalized input frames;
- digest of the ordered output event stream;
- final canonical state digest.

Add optional checkpoint digests every fixed number of `EngineSeq` values. When a regression differs, binary search checkpoints and then compare the first divergent output/state. The checksum detects divergence; it is not a substitute for semantic assertions.

### Replay value

- **Testing:** golden scenarios verify output and final state.
- **Debugging:** a failure can be stopped immediately before its first divergent event.
- **Regression analysis:** an optimization is rejected if outputs or state change unexpectedly.
- **Performance testing:** identical workload content can compare commits and data structures.
- **Incident reproduction:** a captured stream and configuration recreate the decision path, subject to the declared asynchronous durability window.

Cross-machine determinism requires no floating point, no randomized iteration decisions, explicit byte order, checked overflow, fixed protocol versions, and recorded logical time. It does not imply identical physical timing across machines.

## 12. Benchmark Methodology

### Benchmark layers

| Layer | Question | Tool/style |
|---|---|---|
| Codec microbenchmark | Decode/validate one frame and mixed frame batches | Criterion |
| Book microbenchmark | New, cancel, replace, non-crossing, one-level fill, multi-level sweep | Criterion with prebuilt state |
| Risk microbenchmark | Each individual check and accepted/rejected mixes | Criterion |
| Queue benchmark | One-way and round-trip latency, burst throughput, saturation | Custom two-thread harness |
| Core state machine | Orders/sec and trades/sec for fixed deterministic mixes | Criterion for small functions; custom loop for distributions |
| End-to-end pipeline | Feed bytes to output event across real threads | Custom open-loop harness + HDR histogram |
| Replay | Messages/sec with checksum and with/without pacing | Custom harness |
| Soak/backpressure | Queue depth, drops, stalls, memory stability under long load | Custom harness |

Criterion has explicit warm-up, measurement, analysis, and comparison phases,[^15] so it is appropriate for isolated functions. It is not sufficient by itself for an externally paced multi-thread latency distribution.

### Latency boundaries

Report separate measurements rather than one ambiguous “latency”:

- decode latency: bytes available to normalized message;
- queue residence: producer publish to engine consume;
- engine service time: begin applying request to output batch complete;
- order-to-report: inbound request publish to report publish;
- market-to-trade: market event publish to strategy-generated trade publish;
- persistence acknowledgement: output publish to configured journal durability point.

Timestamps should use a monotonic clock. Measure clock-read overhead separately and either subtract only with a justified method or report it as included. Do not use source exchange timestamps for local latency.

### Load generation and coordinated omission

A closed-loop generator that waits for each result before sending the next suppresses offered load during stalls and can hide tail events. The pipeline harness should schedule inputs against an independent monotonic timeline at a fixed or specified burst rate. Record both scheduled send time and actual enqueue time; report when the generator itself falls behind.

Use `hdrhistogram` for p50, p95, p99, p99.9, maximum, sample count, and overflow/error count. The Rust implementation supports coordinated-omission correction,[^16] but correction is not a replacement for a sound open-loop generator. Publish raw and corrected results only when the expected interval and method are stated.

### Workloads

Each result needs a named, versioned workload and input digest. Include at least:

- non-crossing adds with bounded active depth;
- add/cancel-heavy churn;
- same-price FIFO contention;
- mostly rejected risk traffic;
- one-level and many-level crossing;
- market orders with partial remainder cancellation;
- snapshot load followed by incrementals;
- burst larger than the steady arrival rate but below queue capacity;
- intentional queue saturation;
- one symbol initially, then a multi-symbol working set that exceeds small caches.

Report input proportions, initial depth, number of levels, active orders, queue capacities, output multiplicity, and whether the journal/checksum/telemetry is enabled.

### Environment control

Before measurement:

- build the exact commit in the Cargo `bench`/release profile; Cargo's bench profile inherits release by default;[^17]
- warm the code path, allocator state, book, and queues;
- pre-fault or at least touch preallocated memory when evaluating steady state;
- stop unrelated workloads and record unavoidable background services;
- record pinned/unpinned state, CPU isolation, SMT placement, wait strategy, power governor, turbo/boost state, temperature/throttling observations, and virtualization/container status;
- run multiple independent trials and retain raw histograms, not only summaries;
- use the same workload digest for comparisons;
- distinguish cold-start, steady-state, and saturated results.

Every published run must record:

- date and UTC offset;
- repository commit and dirty status;
- CPU model, sockets, physical/logical cores, cache topology if available;
- RAM amount/speed if available;
- operating system and kernel;
- Rust toolchain and target triple;
- compiler profile and flags, including LTO/codegen units/target CPU;
- queue library/version/capacity and wait policy;
- affinity/isolation/SMT/frequency settings;
- workload name, seed, input checksum, event count, and mix;
- warm-up, measurement duration, trials, sample count;
- enabled journal, checksum, tracing, metrics, and allocation instrumentation.

### Profiling and allocations

- Use `cargo flamegraph`/`perf` on Linux and platform-appropriate tools on macOS; `cargo-flamegraph` supports profiling release benchmarks and requires debug information for useful stacks.[^18]
- Record cycles, instructions, branches, branch misses, cache misses, context switches, migrations, page faults, and scheduler time where the platform supports them.
- Count steady-state allocations for individual core operations and the full pipeline.
- Use heaptrack, DHAT, or an equivalent tool for allocation sources outside tight timing runs.
- Profile separately from latency measurement because profiling changes execution.

There are intentionally **no benchmark numbers in this investigation**. Future README tables must be populated only by archived run artifacts from actual hardware.

## 13. Testing and Invariants

### Test layers

1. **Unit tests:** exact examples for protocol fields, price-time rules, state transitions, risk reasons, and arithmetic boundaries.
2. **Golden tests:** fixed binary inputs with expected normalized events, output-event digest, and final-state digest.
3. **Reference-model tests:** compare the optimized indexed book to a deliberately simple `BTreeMap<Price, VecDeque<Order>>` model after every command.
4. **Property tests:** generate valid and invalid command sequences, shrink failures, and verify invariants. `proptest` provides property generation and shrinking.[^19]
5. **Fuzzing:** raw frame decoder, snapshot state machine, structured order-command sequence, and journal recovery. The Rust Fuzz Book documents `cargo-fuzz`/libFuzzer as the standard path.[^20]
6. **Integration tests:** real threads and bounded queues, backpressure, shutdown, replay equivalence, and output persistence.
7. **Soak tests:** bounded memory and stable invariants over long seeded workloads.

### Required scenario matrix

- price priority across several bid/ask levels;
- FIFO time priority at one price;
- partial maker and taker fills;
- full fills and level removal;
- cancel head, middle, tail, only order, unknown order, and already-terminal order;
- replace decrease preserving priority;
- replace increase/price change losing priority;
- rejected replace leaving the original untouched;
- crossing limit with resting remainder;
- market order on empty, shallow, and protected-price-limited books;
- empty-book best-price queries;
- duplicate request retry and conflicting duplicate;
- duplicate client order ID;
- zero, misaligned, overflowing, unknown, and invalid requests;
- every risk rejection and the accepted boundary equal to a limit;
- feed startup, valid snapshot, interrupted snapshot, gap, duplicate, out-of-order, resnapshot, and malformed frame;
- account disable and global kill with cancels still accepted;
- identical replay on repeated runs and different construction/hash seeds;
- capacity exhaustion without allocation or partial mutation.

### Verified invariants

**Matching-book structure**

- Every live order appears exactly once in the order-ID index, one slab slot, and one price-level FIFO.
- Every referenced `prev`/`next` link is reciprocal; a level's head has no predecessor and tail has no successor.
- Level order count and aggregate remaining quantity equal traversal results.
- Every order in a level has the same instrument, side, and price as that level.
- Priority sequences strictly increase along each level FIFO.
- Empty levels are absent; non-empty level head/tail are valid.
- The simulated matching book has `best_bid < best_ask` after each complete command. This invariant does **not** apply to an external `MarketView` while it is unsynchronized or receiving bad data.

**Quantity and lifecycle**

- Every trade quantity is positive.
- While working, `remaining = current_total_quantity - cumulative_filled` and cumulative fill never exceeds current total.
- A replace total cannot be below cumulative fill.
- A terminal order is absent from the live index and can never participate in a later trade.
- Sum of trade quantities attributed to an order equals its cumulative filled quantity.
- At each fill, maker decrease, taker decrease, and trade quantity are equal.
- A market order never rests; its unfilled quantity is terminally cancelled.

The example “total executed quantity cannot exceed submitted quantity” needs refinement because replace can increase total quantity. The correct invariant is that cumulative fill cannot exceed the latest accepted total, and every increase/decrease is represented by an accepted order revision.

**Execution ordering**

- Trade, order, engine, output, and priority IDs are unique and strictly monotonic within their scopes.
- A trade price equals the maker's resting price and satisfies the aggressor's limit/protection bound.
- No worse price executes while a better eligible price has resting quantity.
- At one price, no later-priority order executes before an earlier live order.

**Risk accounting**

- Position changes equal signed completed fills; every trade has equal and opposite buyer/seller quantity in the closed simulator.
- Open-order reservation equals the sum of live remaining quantities/notionals represented by that account's orders.
- Cancel/full fill releases exactly the residual reservation.
- A rejected request changes no order, position, reservation, ID counter that is defined as acceptance-only, or book level.
- Checked arithmetic failure rejects before mutation.
- Account-disable/kill rules never prevent risk-reducing cancellation.

**Replay**

- Re-encoding a decoded valid frame is canonical.
- Replaying identical header/configuration/input yields identical normalized-input digest, complete output bytes/digest, checkpoints, and final state digest.
- Changing pacing alone does not change functional output.
- Truncated or corrupted journals stop at the last explicitly validated frame and never invent a continuation.

Property tests should compare after every step, not only at the end, so the smallest divergent command is visible. Fuzz corpora should retain every discovered crash/divergence as a regression fixture.

## 14. Phase 2 Architecture

### Recommended boundaries

```text
                         +-----------------------------+
                         | TypeScript control process  |
                         | REST + WebSocket + read DB  |
                         +-------------+---------------+
                                       |
                 length-prefixed JSON over local IPC
                                       |
+-------------+    control gateway     v       output/read-model stream
| Rust engine |<---------------->[Rust cold I/O runtime]----------------+
| hot thread  |                                                       |
+------+------+                                                       |
       | ordered non-droppable output                                 |
       v                                                               v
+-------------+                                               +---------------+
| event journal|--------------------------------------------->| SQLite read / |
| + checkpoints|                                              | settlement DB |
+------+------+                                               +-------+-------+
       |                                                              |
       | deterministic trade/batch manifest                           |
       +------------------------------------+-------------------------+
                                            |
                                 +----------+----------+
                                 | optional adapters   |
                                 | Solana and Canton   |
                                 +----------+----------+
                                            |
                                  external slow systems
```

The Rust engine remains the authority for live order, book, position, and risk state. The Node process is an operational projection and command gateway, not a second matching engine. It may host both settlement dispatchers as modules at first; separate adapter processes are warranted only if dependency/runtime isolation becomes operationally useful. This avoids unnecessary microservices.

### Durable outbox boundary

Every confirmed trade produces an immutable settlement fact in the ordered Rust event journal. A Phase 2 projector reads these facts and creates deterministic settlement instructions in a local SQLite database keyed by `SettlementId`. Status transitions are transactional and monotonic:

```text
PENDING -> READY -> SUBMITTING -> SUBMITTED -> CONFIRMED -> FINALIZED
                    |              |              |
                    +------------> UNKNOWN <------+
                                   |
                              FAILED_RETRYABLE

terminal: FINALIZED, FAILED_PERMANENT, CANCELLED_BY_POLICY
```

`UNKNOWN` is not failure and never authorizes a new economic instruction. It means “query the destination using the same identity before retrying.”

### Durability modes

The lab should expose, name, and benchmark two honest modes:

- **Asynchronous journal mode:** the engine emits a report after in-memory execution and the output worker persists later. This minimizes persistence coupling but a process/power failure can lose an acknowledged tail.
- **Durable acknowledgement mode:** the client acknowledgement waits for journal durability, potentially group-committed. This changes the latency boundary and may increase tails.

Neither mode is universally correct. All benchmark/report output must state which acknowledgement boundary it measured. Settlement dispatch can begin only after the trade fact is durable.

## 15. TypeScript Control Plane

### Responsibilities

The TypeScript service is intentionally small:

| Endpoint | Semantics |
|---|---|
| `GET /book/:symbol` | Latest projected levels with `asOfEngineSeq`, feed state, and projection age |
| `GET /orders` | Filtered/paginated projected order summaries; IDs encoded as strings |
| `GET /trades` | Filtered/paginated durable trade events |
| `GET /positions` | Projected positions and reservations with sequence |
| `GET /metrics` | Prometheus/OpenMetrics exposition for control plus latest engine snapshot |
| `GET /health` | Separate process liveness, engine connection, feed freshness, journal lag, projection lag, and settlement health |
| `POST /risk/limits` | Authenticated, validated, idempotent control command; response includes applied engine sequence |
| `POST /engine/kill` | Authenticated emergency latch; idempotent enable/disable semantics and audit event |
| `POST /replay/start` | Starts an isolated replay job and returns a job ID; never reuses the live engine state |

REST is the right fit for low-rate queries and commands. WebSocket is the right fit for best-effort book/health/metric updates. The WebSocket protocol includes a monotonically increasing projection sequence; a gap tells the client to fetch a fresh REST snapshot. Do not pretend a browser WebSocket is the audit log.

### Rust-to-Node interface alternatives

| Interface | Advantages | Drawbacks | Decision |
|---|---|---|---|
| Length-prefixed JSON over Unix socket/named pipe | Simple inspection, easy Rust/TS support, local process isolation, no codegen | More bytes/parsing, manual schema/version discipline | **Initial choice** |
| Loopback TCP + JSON | Portable and easy to debug | Opens a network endpoint and needs port/auth management | Fallback on platforms where local IPC is awkward |
| gRPC/Protobuf | Strong schema and streaming, broad tooling | Code generation and dependency weight for one local peer | Reconsider if API grows materially |
| Shared memory | Low copy and fast | Complex lifecycle, synchronization, crash cleanup, and versioning | Premature |
| Files only | Durable and simple for history | Poor request/response and freshness | Journal remains recovery source, not sole IPC |

Use a 4-byte bounded length prefix, UTF-8 JSON payload, schema version, message type, request/command ID, and engine sequence. Node's `node:net` supports Unix-domain IPC and Windows named pipes.[^21] Encode all `u64`/`u128` values and hashes as strings in JSON because JavaScript `number` cannot exactly represent all 64-bit integers. Apply maximum message sizes and timeouts before parsing.

### API implementation choice

Fastify is a reasonable small framework because it supports TypeScript and JSON-Schema-driven request validation,[^22] and its maintained WebSocket plugin builds on `ws`.[^23] Raw `node:http` would reduce dependencies but moves routing, validation, errors, and lifecycle into project code without benefiting the trading path. Express is familiar but offers less integrated schema validation. The exact versions should be pinned at implementation time after a fresh maintenance/security review.

Do not add an ORM, message broker, service mesh, GraphQL layer, or distributed cache. SQLite is adequate for a single-host operational read model and settlement outbox. Keep SQL explicit and migrations small.

### State access without book locks

The engine emits read-model deltas and periodic full snapshots outside the matching operation. The Rust gateway and Node projector serve these copies. Every response declares `asOfEngineSeq`; exact current-state queries are not worth cloning a large book on the hot thread.

Commands are different: they must reach the engine, be sequenced, applied, journaled, and acknowledged with their outcome. A gateway receipt is not an engine acceptance.

### Process isolation and security

- Bind REST and IPC to loopback/local endpoints by default.
- Require authentication and role checks for every mutation. Separate read, risk-admin, kill, replay, and settlement roles.
- Put TLS at the service or a documented reverse proxy before any non-local exposure.
- Validate JSON bodies, path/query bounds, pagination, and content type; rate-limit expensive snapshots/replays.
- Use an idempotency key plus payload fingerprint on control mutations.
- Audit actor, command ID, old/new value, gateway receipt time, applied engine sequence, and result.
- Do not accept live trading orders through this operational API in the initial scope.
- Redact credentials and do not use order/account IDs as unbounded metric labels.

If Node restarts, the engine continues. Node reconnects, reloads its database checkpoint, replays the durable event journal from the next output sequence, requests a fresh snapshot, and only then reports the projection healthy.

## 16. Observability

### Metrics

| Category | Metrics | Notes |
|---|---|---|
| Feed | decoded frames, malformed frames, last source seq, gaps, duplicates, resyncs, feed state/age | Symbol/source labels are bounded configuration |
| Queues | capacity, sampled depth, high-water mark, full events, producer waits, telemetry drops | Do not scrape queue internals on every message |
| Engine | inputs, accepted/rejected requests, reports, trades, active orders/levels, kill state | Reject reason is a bounded enum label |
| Risk | rejects by rule, position, open reservation, gross exposure, stale-data rejects | Account-level metrics need strict cardinality policy |
| Latency | decode, queue, engine service, order-to-report, persistence ack histograms | Runtime telemetry may sample; benchmark harness is authoritative |
| Process | heartbeat, uptime, build info, thread liveness, projection/journal lag | Health should distinguish liveness and readiness |
| Settlement | pending/submitted/unknown/finalized counts, attempts, age, reconciliation mismatches | Adapter/destination are bounded labels |

`prometheus-client` can encode a registry in OpenMetrics text format,[^24] but exposition belongs on the cold gateway/control process. Prometheus histograms need intentionally selected buckets; the benchmark HDR histogram retains higher-fidelity run data and should not be conflated with production-style telemetry.

### Getting telemetry off the hot path

1. The engine updates plain single-writer counters in its owned state.
2. At a configurable event interval, it copies a small numeric metrics snapshot into the output stream.
3. Exceptional diagnostics are compact numeric events with IDs and sequences.
4. The output/gateway thread converts events to structured logs, metrics, and JSON.
5. Best-effort telemetry has its own bounded queue and may drop with an explicit count; trades/execution reports do not share that drop policy.

Per-order `tracing` spans should be disabled by default. `tracing` supports structured events/spans and can avoid constructing disabled events,[^25] but enabled per-event subscribers still cost work. Use it for lifecycle and cold I/O, and measure any hot-site instrumentation.

### Structured logs

Log JSON or another structured format off-thread with fields such as run ID, engine/output sequence, component, event code, instrument/account/order/trade/settlement IDs, and error category. Do not interpolate large payloads. High-value events include startup configuration digest, feed gap/resync, risk-limit change, kill transition, output backpressure, journal recovery, adapter submission/status, and reconciliation mismatch.

### Latency telemetry

Runtime latency sampling and benchmark measurement have different purposes. Runtime sampling should be configurable (for example every Nth eligible event, with N recorded) to limit clock reads and event volume. Benchmarks record every scheduled operation when the harness can keep up. Queue residence and engine service should be separate so scheduler delay is not blamed on matching code.

## 17. Solana Settlement Adapter

### Role and boundary

The engine executes trades off-chain. Solana holds test collateral and records idempotent settlement batches. The adapter consumes only durable settlement instructions. No RPC request, signature, transaction simulation, blockhash lookup, confirmation poll, or program call can affect matching latency or change whether a trade executed.

### Recommended small Anchor program

Instructions:

- `deposit_collateral(amount)`
- `withdraw_collateral(amount)`
- `settle_batch(settlement_id, manifest_hash, entries)`
- `freeze_account(account_id, frozen)`

Accounts:

| PDA/account | Suggested seeds | Contents/purpose |
|---|---|---|
| `Config` | `["config"]` | admin, settlement authority, pause state, allowed mints/programs, version |
| `VaultAuthority` | `["vault"]` | PDA authority for custody token accounts |
| `CollateralAccount` | `["collateral", owner, mint]` | owner, mint, available/internal amount, frozen flag, bump, version |
| Vault token account | ATA or PDA-owned token account per mint | Actual escrowed tokens controlled by `VaultAuthority` |
| `SettlementReceipt` | `["settlement", settlement_id]` | manifest hash, submitter, entry count, result/slot metadata where available |

PDAs are deterministic addresses controlled by the deriving program, with no corresponding private key,[^26] so they provide isolation and replay-address identity. Anchor constraints should verify signer, seeds/bump, owner, fixed address, mint, token authority, and token program; Anchor documents these checks directly.[^27] Never rely on the client to pass the right account.

### Deposit and withdrawal

`deposit_collateral` transfers supported tokens from the user's token account into the program-controlled vault using `transfer_checked`, then credits the internal collateral account. `withdraw_collateral` requires the owner, an unfrozen account, sufficient available amount, and a supported mint, then signs the vault transfer with PDA seeds. Anchor's token interface can call either the original Token Program or Token-2022 using the same checked-transfer pattern.[^28]

Phase 1 of the Solana adapter should support the original SPL Token program and/or a strict allowlist of Token-2022 mints with explicitly reviewed extensions. Token-2022 extensions are optional, can add state and behavior, and some combinations are incompatible.[^29] Do not claim generic Token-2022 support. In particular, transfer fees, transfer hooks, confidential balances, permanent delegates, default frozen state, and pausable mints can invalidate simple “requested amount equals received amount” assumptions.

The safest first lab policy is to reject unsupported extensions and require exact token balance deltas. Add extension-specific behavior only with dedicated tests.

### Batch settlement

The off-chain coordinator creates a canonical manifest:

- version and destination;
- deterministic batch/settlement ID;
- ordered constituent trade IDs and their trade-log digest;
- ordered debit/credit entries in token base units;
- per-mint zero-sum totals;
- creation/expiry policy and prior batch link if used.

Before submission, verify off-chain that entries are unique/canonical, every amount is positive, per-mint debits equal credits, no account goes negative, and the transaction fits Solana account/compute/size limits. The program repeats all safety-critical checks; off-chain validation only improves errors.

`settle_batch` must:

1. verify the authorized settlement signer and global/account state;
2. derive the receipt from `settlement_id` and require it to be absent or exact-match idempotent;
3. verify the supplied manifest hash and canonical bounded entries;
4. use checked arithmetic and prevent negative collateral;
5. require debit/credit conservation for each mint;
6. apply all ledger balance changes in one Solana transaction;
7. create the receipt so the same ID cannot apply twice.

Solana executes all instructions in a transaction atomically,[^30] but one transaction has bounded size and compute. Therefore “batch” cannot mean unlimited. Net a deterministic group of trades off-chain into a bounded set of account/mint deltas, and split only at an explicit batch boundary. Do not split one promised atomic DvP set across transactions.

Per-trade settlement is easy to map but creates more submissions and failure points. Deterministically batched/netted settlement is preferred, provided the manifest preserves every constituent trade and the lab clearly states the assumed netting model. This is an engineering demonstration, not a legal netting claim.

### Idempotency and unknown status

The PDA receipt is the business idempotency guard. The adapter uses the same `settlement_id` and manifest hash for every retry. Same ID/different manifest is a permanent conflict.

After a timeout:

1. query the receipt PDA;
2. query the original transaction signature with `getSignatureStatuses`/transaction lookup; current Solana documentation directs clients to `getSignatureStatuses` for confirmation state;[^31]
3. if neither proves an outcome and the blockhash is still valid, rebroadcast the identical signed transaction;
4. after expiration, build a new transaction/signature containing the same settlement ID and manifest;
5. never create a new settlement ID to escape uncertainty.

Recent blockhashes expire and RPC nodes can lag, which is why submission and confirmation must be stateful and reconciled rather than treated as one request.[^32] Mark `FINALIZED` only at the configured commitment. Record processed/confirmed separately if displayed.

### Reconciliation

Periodically compare:

- every durable off-chain settlement instruction to a receipt/status;
- receipt manifest hash to the local canonical manifest;
- each vault token balance to the sum of program collateral balances for that mint;
- engine settlement ledger totals to finalized destination batches;
- deposits/withdrawals observed on-chain to credited/debited off-chain records.

A mismatch halts settlement for the affected mint/account and raises an operational incident. It does not roll back matching-engine trades.

## 18. Canton Settlement Adapter

### Role

Canton is an optional institutional post-trade destination:

```text
durable confirmed trade(s)
  -> canonical settlement instruction
  -> participant allocation/authorization workflow
  -> atomic DvP transaction
  -> ledger update/offset
  -> local reconciliation
```

It is never queried by risk or execution. Unavailability accumulates pending settlements and may cause a separately configured collateral/settlement-risk halt, but it does not add network latency to a match already in progress.

### Standard choice

Prefer the Canton Network Token Standard instead of inventing a private holding/token API. CIP-0056 defines metadata, holdings, transfer-instruction, allocation, allocation-request, and allocation-instruction APIs and explicitly supports all-or-nothing multi-transfer DvP through allocations.[^33]

As of this investigation, CIP-0112 is approved and defines Token Standard V2 improvements for privacy-preserving batch settlement, accounts, committed allocations, iterated settlement, and standardized batching.[^34] Phase 2 implementation should target the deployed version supported by the selected Canton environment, isolate v1/v2 translation behind the adapter, and pin package/API versions. “Approved standard” does not prove that every participant/registry the lab might connect to has deployed it.

### Daml application contracts

Do not implement another token. Implement only the small venue workflow that the standard does not provide:

- `SettlementBatch` or `TradeSettlementInstruction`: canonical settlement ID, venue/operator, participant accounts/parties, instrument IDs, exact decimal legs, constituent trade digest, created time/deadline, and adapter version.
- A consuming `Settle`/`Complete` choice that can run only with the standard allocations and required executor authority.
- A consuming `Expire`/`Cancel` path whose controller and deadline policy are explicit.
- Optional `SettlementResult` evidence only if it adds query value beyond ledger transactions; avoid mirroring redundant mutable state.

The venue/operator can be the signatory for a record of off-chain execution, but it cannot manufacture participant authorization. Participants/custodians authorize through allocation workflows. Daml signatories consent to contract creation, observers gain visibility, and choice controllers authorize exercises.[^35] These roles must model actual responsibilities, not be added broadly for convenience.

### Holdings, allocations, and DvP

1. Translate a durable off-chain batch into exact standard `TransferLeg` obligations.
2. Create or request allocations from the holders/accounts required to deliver each asset.
3. Observe allocation state through the participant Ledger API and registry interfaces.
4. When all required allocations exist and agree on settlement identity/legs, the authorized executor submits one transaction exercising all transfer legs.
5. Canton/Daml transaction atomicity makes the legs all succeed or all fail; CIP-0056 describes this one-transaction DvP model.[^33]
6. Persist update ID, record time, offset, command ID, and resulting holding/allocation contract IDs in the same local DB transaction that advances the ingestion checkpoint.

V2 committed allocations could represent prefunded trading collateral and permit iterated settlement,[^34] but using them changes custody/trust and withdrawal-race assumptions. Start with exact per-batch allocations unless the prefunding model is explicitly selected and tested.

### Authorization and privacy

- Use opaque Canton `Party`/account identifiers; do not parse or hardcode their internal format.
- Give the operator/executor visibility necessary to coordinate the full settlement.
- Give each participant only the obligations and results it needs.
- Give each asset registry/custodian visibility into its own asset legs, not unrelated legs.
- Avoid making every party an observer of one broad contract; Daml privacy propagates transaction consequences to stakeholders, so excessive observers leak relationships.[^36]
- Authenticate Ledger API access and scope the local projection by party. A read database containing multiple parties can leak data if queries are not party-filtered.[^37]

### Idempotency and lifecycle

Use one stable application `SettlementId` and manifest digest through every stage. A keyed/unique active settlement contract plus a consuming settle choice provides business-level replay protection. Canton command deduplication is useful transport protection—the Ledger API considers a command duplicate within its deduplication period when its change ID matches[^38]—but it is not a permanent substitute for the settlement contract's uniqueness.

Lifecycle states in the local coordinator:

```text
PENDING_ALLOCATION -> PARTIALLY_ALLOCATED -> READY
       |                                      |
       +-> EXPIRED / REJECTED                 v
                                      SUBMITTED -> SETTLED
                                           |
                                        UNKNOWN
```

Store the last consumed ledger offset transactionally with projection changes. On restart, subscribe from that offset, tolerate redelivery by update/contract ID, and reconcile active allocations/holdings before dispatch.

### Reconciliation

- Map every finalized local settlement to a Canton update and settlement contract consumption.
- Map every active/archived allocation to its local batch and participant leg.
- Recompute holdings/position deltas from ledger events and compare to local finalized deltas.
- Detect expired, withdrawn, or rejected allocations and move the local batch out of `READY`.
- Treat participant/registry/synchronizer unavailability as retryable without changing the settlement ID.
- Escalate manifest mismatch, unexpected leg, conflicting active ID, or unauthorized party as permanent/security faults.

## 19. Failure Recovery and Reconciliation

### Core rule

Execution and settlement are different finality domains. Once the Rust engine emits a confirmed trade, a destination failure does not “untrade” it. The system records a settlement obligation, retries or repairs it, manages exposure operationally, and reconciles. Any compensating trade would be a new authorized trade, not automatic rollback.

### Failure matrix

| Failure | Detect | Recovery | Duplicate defense |
|---|---|---|---|
| Rust engine succeeds, settlement fails | Trade durable; destination status retryable/permanent | Keep trade final, mark obligation pending/failed, retry or manual repair, optionally halt affected account/mint | Stable settlement ID + manifest hash |
| Solana RPC unavailable | Timeout/transport errors, stale health | Backoff with jitter, query another configured RPC if policy allows, resume same instruction | Receipt PDA and same settlement ID |
| Canton participant unavailable | Ledger API disconnect/health | Reconnect from persisted offset; rebuild active view; resubmit same logical command only after status query | Settlement contract identity + command/change ID |
| Node control service restarts | IPC disconnect and heartbeat loss | Rust engine continues; Node loads DB checkpoint, tails journal, requests snapshot, then becomes ready | Output sequence/update ID transactional projection |
| Settlement submitted twice | Existing receipt/contract or duplicate command | Return existing status if manifest matches; permanent conflict if it differs | Destination business key, not only HTTP retry key |
| Settlement status unknown | Submit timed out without authoritative result | Enter `UNKNOWN`; query receipt/signature/update/active contracts; never mint a new instruction | Same ID and canonical fingerprint |
| Adapter crashes after submission before DB update | Local row remains `SUBMITTING` | Recovery worker queries destination first, then advances or retries | Same destination ID |
| Process crashes before request acknowledgement | Client has no definitive result | Retry same request ID; engine/result journal returns prior outcome when durable | Client sequence + fingerprint/result cache |
| Engine crashes before asynchronous journal persistence | Recovered log lacks acknowledged tail | Explicitly disclose loss window; clients reconcile orders and restart halted | Durable-ack mode removes this window at latency cost |
| Journal frame is truncated | CRC/length/EOF failure | Recover only through last complete frame, report tail loss, halt automatic settlement past it | Frame sequence/CRC and checkpoint digest |
| Output queue remains full | High-water/full timer and health fault | Stop taking new risk, keep draining, operator repair/restart | Non-droppable ordered queue |
| Snapshot/resync never completes | Feed state/age | Keep risk-increasing orders rejected; retry source or stop run | Feed epoch + source sequence |

### Idempotent boundary pattern

Every mutation crossing a process or external system uses:

- stable operation ID;
- canonical payload and fingerprint;
- durable local state before/after submission;
- monotonic status transitions;
- query-before-retry after uncertainty;
- destination-side replay guard where possible;
- transactional checkpoint update with the projected result.

The HTTP `Idempotency-Key`, Rust `RequestId`, Solana `SettlementReceipt` PDA, Canton settlement contract/key, and local database unique key should all represent the same logical operation at their respective boundaries.

### Reconciliation loops

Run reconciliation independently of normal dispatch:

1. scan nonterminal local operations oldest first;
2. query destination by stable ID/status evidence;
3. compare canonical fingerprint and economic fields;
4. advance status if authoritative evidence exists;
5. retry only a known-not-applied or safely idempotent operation;
6. quarantine conflicting evidence and alert;
7. record attempt, source, observed destination state, and next retry time.

Backoff should be bounded and include jitter outside deterministic core logic. Tests use a fake clock and scripted adapters so failure sequences are repeatable.

## 20. Proposed Repository Structure

The example structure in the brief has too many likely one-module crates (`protocol`, `market-data`, `orderbook`, `matching`, `risk`, `engine`, `replay`) for an initial project. Crate boundaries should represent compilation/dependency/ownership boundaries, not every noun.

Recommended future structure:

```text
rust-low-latency-trading-lab/
├── Cargo.toml                       # workspace, created in Phase 1 implementation
├── crates/
│   ├── protocol/                    # stable domain/wire types and manual codec
│   │   ├── src/
│   │   └── tests/                   # golden codec tests
│   ├── trading-core/                # market view, order book, matching, risk, state hash
│   │   ├── src/
│   │   │   ├── market.rs
│   │   │   ├── book.rs
│   │   │   ├── matching.rs
│   │   │   ├── risk.rs
│   │   │   ├── replay.rs
│   │   │   └── lib.rs
│   │   ├── tests/                   # model/property/replay tests
│   │   └── benches/                 # core Criterion benchmarks
│   └── engine-runtime/              # OS threads, queues, recorder, cold control gateway
│       ├── src/
│       ├── tests/                   # threaded/backpressure tests
│       └── benches/                 # pipeline/custom harness entry points
├── apps/
│   ├── simulator/                   # Rust generator, capture conversion, replay CLI
│   └── control-api/                 # Phase 2 TypeScript REST/WS/projector
├── adapters/
│   ├── solana/
│   │   ├── program/                 # small Anchor program
│   │   └── client/                  # adapter module and reconciliation tests
│   └── canton/
│       ├── daml/                    # venue settlement workflow only
│       └── client/                  # Ledger API/token-standard integration
├── fixtures/
│   ├── feeds/                       # tiny licensed/generated golden binary inputs
│   └── expected/                    # canonical outputs/checksums
└── README.md                        # normal project documentation during implementation
```

Rationale:

- `protocol` is separate because both core/runtime and cold external components need stable messages without pulling in engine implementation.
- `trading-core` keeps book, matching, risk, and replay state together because they share one ownership/invariant boundary and should test as one pure state machine.
- `engine-runtime` separates concurrency/I/O dependencies from the pure core.
- `simulator` is a binary, not another general-purpose library.
- Criterion benches live beside the code they measure; there is no “benchmarks crate” unless a later cross-workspace harness needs one.
- Solana program and Canton Daml code need their own toolchains and deployment layouts, but their clients can initially run as modules in the control process.
- Avoid a generic `common`, `utils`, or `services` directory. Move code only when two concrete consumers and a stable boundary exist.

During the current investigation, none of this structure should be created. This document is the only added file.

## 21. Dependencies and Alternatives

Versions must be selected and locked at implementation time after checking MSRV, license, advisories, release status, and transitive dependencies. The table recommends roles, not unreviewed version pins.

### Rust runtime and core

| Dependency | Purpose | Why/alternative | Hot path? |
|---|---|---|---|
| Standard library | `BTreeMap`, `HashMap`, `Vec`, integer/time/thread primitives | Prefer it wherever it is sufficient | Yes |
| `slab` | Preallocated uniform order-node storage with stable indexes while occupied | Simpler than a custom arena; enforce capacity because it otherwise grows and reuses keys[^7] | Yes |
| `rtrb` | Bounded SPSC queues | Topology matches exactly; compare with `ArrayQueue`, bounded channel, and std baseline[^9] | Yes |
| `crossbeam-queue` | `ArrayQueue` comparison or later MPMC boundary | Mature general bounded queue but more general than SPSC[^10] | Benchmark candidate |
| `crossbeam-utils` | Cache padding for project-owned coordination fields | Avoid hand-picked alignment constants where appropriate[^11] | Small use |
| `thiserror` | Concrete typed library errors | Derives standard `Error` without entering public API; handwritten enums are an alternative[^39] | Error paths only |
| `crc32fast` | Accidental frame/journal corruption detection | Simple CRC32 implementation; not cryptographic authentication[^40] | Decode/journal, benchmark |
| `blake3` | Canonical input/output/state digests | Stable specified digest; SHA-256 is slower but more ubiquitous; std has no stable content hash | Replay/checkpoints, normally cold |
| `serde` + `serde_json` | Configuration/control JSON and cold metadata | Mature ecosystem; keep out of market-data and matching path[^41] | No |
| `postcard` | Optional compact cold record alternative | Stable format and caller-provided buffer support; manual format preferred for feed[^5] | Not initially |
| `clap` | Simulator/replay/benchmark CLI | Clear typed command parsing; manual args save a dependency but add boilerplate | No |
| `tracing` + subscriber | Structured lifecycle/cold I/O diagnostics | Standard `log` is simpler; tracing fields/spans are useful in async control code[^25] | Disabled/minimal hot sites |
| `prometheus-client` | OpenMetrics registry/exposition | Direct, typed Rust metrics; Node `prom-client` is an alternative aggregation point | No exposition in hot path |
| `hdrhistogram` | Tail-latency recording and run artifacts | Supports broad range/precision and omission correction[^16] | Benchmark harness; sampled runtime only |
| `criterion` | Statistical microbenchmarks and comparisons | Custom harness for multi-thread distributions[^15] | Dev only |
| `proptest` | Stateful/model property tests and shrinking | QuickCheck is simpler; proptest offers richer strategies/shrinking[^19] | Dev only |
| `cargo-fuzz` / `libfuzzer-sys` | Parser and state-machine fuzzing | AFL is an alternative; cargo-fuzz is well documented in Rust ecosystem[^20] | Dev tool only |
| `core_affinity` | Optional thread placement experiment | OS-specific calls offer more control but require unsafe/platform code; do not enable by default[^12] | Startup only |
| `tokio` | Phase 2 Rust control IPC and slow asynchronous I/O | Standard blocking threads are enough for Phase 1; isolate runtime from engine[^8] | No |

Do not initially add `hashbrown`, `smallvec`, `bytes`, `parking_lot`, Rayon, an async channel, an alternate allocator, a decimal crate, or a generic serialization framework unless a concrete use exceeds the standard library baseline.

Do not use `anyhow` in library APIs. It is acceptable at binary top-level for adding operational context, while core error categories remain concrete and machine-readable.

### Explicitly rejected or deferred Rust dependencies

- **`bincode`: rejected for a new protocol** because the current project is explicitly unmaintained.[^4]
- **FlatBuffers/Cap'n Proto/Protobuf:** deferred; useful when schema breadth and cross-language evolution justify code generation.
- **`zerocopy`: deferred** until parsing is profiled; its safe derives and byte-order types make it the preferred experiment before project-authored unsafe.[^3]
- **Custom allocator:** deferred until allocation evidence identifies a relevant path.
- **LMAX-style custom Disruptor clone:** rejected initially; the topology needs only clear SPSC ownership.

### Phase 2 TypeScript and adapter dependencies

| Dependency/tool | Purpose | Recommendation |
|---|---|---|
| Node.js active LTS + TypeScript | Control process | Pin supported versions; use strict compilation |
| Fastify | REST routing and JSON Schema validation | Prefer over hand-built HTTP plumbing for a small operational API[^22] |
| `@fastify/websocket` / `ws` | Best-effort telemetry stream | One WebSocket route; no event-bus abstraction[^23] |
| Prometheus client for Node or Rust projection | `/metrics` | Choose one exposition owner to avoid duplicate metric truth |
| SQLite driver / supported `node:sqlite` | Read model and settlement outbox | Decide against selected Node LTS; explicit SQL, no ORM |
| Anchor + `anchor-spl` | Solana program and token CPI constraints | Pin to a Solana/Anchor-compatible toolchain and audit generated accounts |
| Solana client SDK | Submission/status/account queries | Lives only in adapter; record commitment and RPC behavior |
| Daml SDK/Canton Ledger API and Token Standard packages | Canton contracts/client | Pin exact environment-compatible v1/v2 package IDs and APIs |

Supply-chain policy should include lockfiles, license review, `cargo audit`/RustSec, npm audit/advisory review, minimal feature flags, and a documented update cadence. “Actively maintained” must be rechecked when implementation starts.

## 22. Performance Risks

| Risk | Likely symptom | Measurement | Initial mitigation |
|---|---|---|---|
| New `BTreeMap` level allocation | Rare high add/replace tails | Allocation trace correlated with price churn | Pre-size other stores; compare pooling/ladder only if material |
| Hash-map resize | Latency cliff under order/account growth | Allocation counter and capacity telemetry | Reserve and enforce max capacity |
| Output amplification on sweeps | Output queue fills; engine stalls | Reports per command, queue high-water, max latency | Size for workload; document non-drop backpressure |
| Consumer slower than engine | Growing queue depth and burst stalls | Producer-wait time and depth histogram | Faster/offloaded journal, capacity, optional batch writes |
| False sharing | Higher cache invalidations, poor scaling | Hardware counters; padded/unpadded comparison | Separate producer/consumer coordination fields |
| Busy spin and SMT contention | Thermal throttling or sibling slowdown | CPU frequency/temp and paired workload | Core isolation, adaptive default, benchmark topology |
| Tokio/control runtime interference | Tail spikes despite fast core service | Per-stage latency and scheduler counters | Separate threads/cores; disable control in baseline |
| Logging/metrics enabled per event | More instructions/allocations | on/off benchmark and flamegraph | Numeric snapshots, sampling, off-thread formatting |
| Clock reads | Engine service-time inflation | Benchmark timestamp instrumentation on/off | Stamp at required boundaries only |
| Branch-heavy validation | Higher p99 on mixed invalid traffic | Accepted/reject mix benchmarks and branch misses | Fixed straightforward check order; avoid clever branch tricks |
| Cache-working-set growth | Tail degradation with depth/symbols | sweep active orders/levels/symbols and cache counters | Compact nodes; shard only after measurements |
| Page faults/cold memory | Cold-start and burst outliers | major/minor faults and cold/warm split | Preallocate and touch memory for steady-state tests |
| CPU frequency/power behavior | Run-to-run instability | record governor/turbo/temp/frequency | Isolated repeatable benchmark host |
| Batching | Better throughput but worse event wait | batch-size sweep with open-loop latency | Publish latency/throughput frontier, not one “best” number |
| Journal `fsync` | Large persistence-ack tails | async vs per-event vs group-commit | Separate durability modes and boundaries |
| Canonical hashing | Replay throughput reduction | checksum on/off replay runs | Checkpoint interval; final hash mandatory, per-event optional |
| Direct-index ladder size | Cache/TLB waste despite `O(1)` | range/depth/memory sweep | Keep sparse tree baseline |
| Alternate allocator | No benefit or new jitter | allocation counts and A/B run | Default allocator until evidence |

No single configuration optimizes latency, throughput, power, and durability. Results should present a curve across offered loads and configurations. At saturation, throughput may remain high while queue residence and tails diverge; that knee is more informative than peak messages/sec alone.

## 23. Security and Correctness Risks

### Protocol and resource risks

- Integer overflow in price × quantity, scale conversion, aggregate quantity, position, exposure, counters, or IDs. Use checked operations and reject before mutation.
- Memory exhaustion from untrusted frame length, snapshot level count, unique price churn, order capacity, dedup keys, control response size, or metric labels. Bound each explicitly.
- Parser differential/canonicalization errors. Maintain golden bytes, round trips, raw fuzzing, and a canonical encoder.
- CRC confusion. CRC detects accidental corruption; it does not authenticate an attacker. Authentic network transport needs authentication/TLS outside the v1 file codec.
- Hash-map randomness affecting output. Never iterate unordered containers for decisions or canonical serialization.
- Partial replace/risk mutation. Validate all fields and deltas before modifying existing state.
- Stale reference price admitting risky market orders. Reject on missing, gapped, or logically stale market data.
- Feed snapshot race. Build scratch state and swap only after matching `SnapshotEnd` and buffered sequence validation.

### Operational/API risks

- Unauthorized kill, risk-limit, replay, withdrawal, settlement, or freeze commands. Apply authentication, least-privilege roles, local binding, audit, and idempotency.
- Control projection presented as exact current state. Include engine sequence and age in every response.
- Kill switch delayed by queue saturation. Use the Rust gateway latch and record its observed engine boundary.
- Kill switch reset after restart. Restart in a documented fail-closed/halted state until configuration and journal recovery complete.
- Replay endpoint consuming the engine core during a latency run. Isolate replay jobs and disable them in controlled benchmarks.
- Secrets in logs or metrics. Use numeric operational IDs, redact credentials, and cap label cardinality.

### Settlement risks

- Double submission after timeout. Stable settlement identity, query-before-retry, and destination replay guards are mandatory.
- Same ID with different economics. Store/verify a canonical manifest hash and treat mismatch as a security fault.
- Off-chain/on-chain rounding. Convert integer lots/base units using explicit scales; reject non-exact conversion.
- Solana account substitution, wrong mint, wrong token program, malicious Token-2022 extension, signer confusion, or PDA collision assumptions. Validate every account/authority/program and allowlist supported mint behavior.
- Solana “confirmed” mistaken for final. Store each commitment transition and use a configured finality rule.
- Canton over-disclosure. Minimize observers and project queries by authorized party.
- Canton v1/v2 mismatch or registry-specific behavior. Discover/pin supported APIs and keep translation versioned.
- Netting assumptions. A technical net sum is not evidence of legal novation or enforceable netting; label the lab model.
- Frozen/disabled account with existing obligation. Define whether settlement is blocked or operator-authorized; surface any stranded obligation.

### Engineering-quality rules for implementation

Implementation must look like ordinary professional engineering work:

- Use straightforward Rust, clear ownership, small focused domain types, explicit state transitions, and predictable control flow.
- Do not add unnecessary abstraction, generic parameters, traits, wrapper structs, helper layers, repeated validation, or defensive code without a stated failure.
- A domain newtype is justified when it prevents mixing values such as `OrderId` and `TradeId`; a wrapper that only forwards methods is not.
- Keep validation at trust/ownership boundaries. Internal functions may rely on documented invariants rather than rechecking every field.
- Avoid clever code without a measured benefit and a reference implementation.
- Keep hot/cold paths visually and dependency-wise distinct.
- Fail explicitly on capacity and arithmetic limits; do not silently grow, wrap, truncate, or drop.

Source comments must use short, simple English and explain only an invariant, non-obvious decision, safety rule, protocol rule, or measured performance choice. Examples of appropriate comments are `// Keep price-time priority.`, `// Reject stale orders.`, and `// Hot path: avoid allocation.` Do not comment obvious code, every function, or restate types. Avoid essay comments, decorative banners, and claims about performance. Public APIs may use short normal Rust documentation comments where useful.

## 24. Implementation Order

There are exactly two phases. Items below are ordered to keep a runnable, testable vertical slice and to delay optimization until a baseline exists.

### Recommended Phase 1 implementation order

1. Define instrument scales, integer domain types, input/output schemas, order states, reject enums, ID/sequence rules, and canonical event ordering.
2. Specify and implement the manual binary header/frame codec with golden bytes, strict bounds, CRC, and raw decoder fuzz target.
3. Build the seeded market/order generator and normalized event log writer/reader.
4. Implement a slow reference matching book with precise market/limit/cancel/replace semantics and table-driven tests.
5. Implement the production safe indexed-FIFO + `BTreeMap` book and compare it step-by-step to the reference model.
6. Add deterministic matching, trade/report ordering, replace priority rules, and lifecycle invariants.
7. Add account positions, working-order reservations, fixed-order pre-trade risk checks, deduplication, stale-feed behavior, and kill/account controls.
8. Compose the pure single-thread `TradingCore` state machine, reusable output buffer, and canonical state/output checksums.
9. Implement snapshot/gap state machine and deterministic functional/step/paced replay; add checkpoint divergence reporting.
10. Add dedicated feed/engine/output threads with bounded SPSC queues, adaptive wait, backpressure, clean shutdown, and threaded tests.
11. Add allocation counting, Criterion microbenchmarks, custom open-loop pipeline harness, HDR artifacts, environment capture, and profiling workflow.
12. Run real baselines, archive raw results, compare queue/wait/affinity/data-structure hypotheses, and only then accept targeted optimizations.

### Recommended Phase 2 implementation order

1. Define versioned Rust-control IPC messages, command idempotency, `asOfEngineSeq`, health semantics, and authentication roles.
2. Add the cold Rust Tokio gateway and read-model snapshot/delta output without changing engine ownership.
3. Implement the TypeScript Fastify REST endpoints, WebSocket gap/resnapshot behavior, and restart recovery from journal plus snapshot.
4. Add off-thread structured logs, numeric metric snapshots, Prometheus exposition, queue/feed/risk/execution health, and telemetry-drop tests.
5. Add SQLite durable projection/outbox, settlement IDs/manifests, state machine, retry scheduler, and fake-destination reconciliation tests.
6. Implement and test the minimal Anchor collateral program on a local validator, initially restricting supported token behavior.
7. Implement the Solana dispatcher, confirmation/unknown/retry recovery, receipt reconciliation, vault conservation checks, and bounded batch tests.
8. Implement the optional Canton Daml settlement workflow against the chosen deployed Token Standard version, then Ledger API projection, allocation lifecycle, DvP, privacy, deduplication, and recovery tests.
9. Run end-to-end failure injection for service restarts, lost acknowledgements, duplicate submissions, unavailable RPC/participant, unknown status, corrupted journal tail, and reconciliation mismatches.

Phase 2 does not begin by putting APIs around unfinished core state. Phase 1's journal, identities, replay, and ordered outputs are prerequisites for safe settlement integration.

## 25. Decisions We Should Measure Instead of Assume

1. **`rtrb` versus `ArrayQueue` versus bounded standard/crossbeam channels:** measure one-way/round-trip p50–max, burst saturation, idle CPU, and backpressure behavior on the target host.
2. **`BTreeMap` versus a bounded direct-index ladder:** sweep active price range, sparsity, churn, orders per level, memory, allocation, cache misses, and tails.
3. **Indexed FIFO versus `VecDeque` reference/tombstones:** measure cancellation/replace mixes and cleanup spikes, not just sequential matching.
4. **Adaptive park versus busy spin:** measure latency distribution, CPU consumption, frequency/thermal behavior, and impact on sibling cores.
5. **Pinned versus unpinned threads and SMT placement:** measure migrations/context switches and multi-trial variance; do not infer from one run.
6. **Queue capacity:** sweep bursts and consumer stalls; identify the saturation knee and memory cost.
7. **One-event versus batched queue operations:** measure throughput gain against queue-residence and p99.9 cost.
8. **Manual checked decoder versus `zerocopy`:** measure codec share of end-to-end cycles and validate safety/maintainability before changing.
9. **CRC per frame versus block/file CRC:** measure corruption-detection granularity and decode cost; retain deterministic error localization.
10. **State checksum every event versus checkpoints/final only:** measure replay slowdown and divergence-debug value.
11. **Telemetry off, sampled, and every event:** quantify clock, counter, queue, and subscriber cost separately.
12. **Risk mark recalculation strategies:** compare incremental aggregate maintenance to scanning positions for varying symbol/account counts.
13. **System allocator versus any alternative:** only after proving relevant steady/burst allocations remain.
14. **Cargo profile settings:** compare default release/bench, target CPU, LTO, codegen units, panic strategy, and debug info while recording compile portability.
15. **Asynchronous, per-event durable, and group-commit journal acknowledgement:** measure both order-to-report and persistence-ack distributions plus throughput.
16. **Public crypto JSON normalization versus native binary replay:** measure only as ingest tooling; do not let it redefine engine hot-path claims.
17. **Control snapshot frequency:** measure engine copy time, output volume, projection age, and query usefulness.
18. **Per-trade, gross-batch, and netted settlement:** compare destination transaction count, manifest/reconciliation complexity, failure scope, and confirmation time—not execution latency.
19. **Solana original SPL Token versus allowlisted Token-2022 support:** measure program/account complexity and test surface; selection is primarily correctness/security, not speed.
20. **Canton Token Standard v1 versus v2 adapter:** decide from actual environment support, workflow/privacy needs, and integration tests.

No unsafe optimization is eligible merely because a microbenchmark is faster. It must improve a project-level metric beyond noise under a representative workload and preserve the reference-model/replay results.

## 26. Open Questions

Defaults below allow implementation to begin; changing one must update fixtures and protocol/version decisions.

### Phase 1 product/model questions

- **Initial scope:** one instrument per run, with schemas/data structures capable of configured multiple instruments later. This keeps first invariants small.
- **Self-trade policy:** default to explicit `Allow` for the closed simulator or implement one `CancelAggressor` mode; choose before golden outputs.
- **Limit time in force:** default GTC; market orders are IOC by definition. Decide whether limit IOC belongs in Phase 1 before schema freeze.
- **Replace priority:** proposed rule is same-price decrease keeps priority; increase or price change loses it. Confirm this venue model.
- **Market protection reference:** choose midpoint, opposite best, or last trade and collar units. Default to midpoint when both sides are valid.
- **Trade fees:** exclude initially. If added, use exact integer fee units and include them in risk/settlement invariants.
- **Position ownership:** define whether the simulator is closed with both maker/taker accounts tracked. Recommended yes, enabling quantity conservation.
- **Feed duplicates:** decide whether exact sequence duplicates are ignored or fatal in strict replay. Recommended record-and-ignore duplicates only in live normalization; strict normalized replay should contain the recorded outcome once.
- **Maximum live orders, levels, frame size, snapshot levels, and fills per command:** choose conservative fixture defaults, make configurable, and record them.
- **Client session reset:** define how a new session establishes its monotonic request sequence and prevents replay from an old session.

### Runtime and durability questions

- Which Linux machine will be the primary published benchmark host, and can cores/frequency be controlled?
- Is macOS a development-only environment with Linux as the claim environment? Recommended yes for repeatable affinity/perf counters.
- Which acknowledgement mode is the default demo? Recommended asynchronous with a prominent durability statement, plus a separately measured durable mode.
- What output-journal flush/group-commit policy is acceptable for tests and demos?
- Should emergency kill be settable through a second local Rust-only path if Node is unavailable? Recommended eventual small operator CLI using the same authenticated IPC, not a new service.

### Phase 2 control and settlement questions

- What authentication mechanism will protect the control API in the demo: local bearer token, mTLS, or reverse-proxy identity? Loopback bearer token is the minimal demo; never expose it publicly by default.
- Which Node LTS and SQLite interface are selected at implementation time?
- What projection retention/pagination limits are needed for orders and trades?
- How are settlement batches closed: fixed trade count, recorded logical-time window, or explicit command? Fixed count or explicit recorded boundary is most deterministic.
- What is the assumed settlement asset pair and netting model? It must be stated before `settle_batch` economics are coded.
- Which Solana cluster and commitment define demo finality? Local validator first; public devnet only as an integration demonstration.
- Which Token-2022 extensions, if any, are supported? Default none until dedicated validation exists.
- Does `freeze_account` block existing settlements or only withdrawals/new obligations? This needs an explicit operational policy.
- Who controls Solana admin and settlement authorities, and how are keys rotated? A local test key is sufficient for the lab; production key management is a non-goal.
- Which Canton environment and deployed Token Standard package versions are available? Detect and pin rather than assume CIP approval equals deployment.
- Are Canton participants pre-funded with exact per-batch allocations, or are V2 committed allocations used? Exact allocations are the simpler default.
- Which parties must see the complete Canton batch, and which should see only their legs?
- What finality/evidence is sufficient to move Canton status to `SETTLED` and to release local pending exposure?

None of these questions requires a third phase. They select policies within Phase 1 or Phase 2.

## 27. Final Recommended Architecture

### Major architectural decisions

- Build a pure, single-thread-owned Rust trading state machine and wrap it with three dedicated OS threads connected by bounded SPSC queues.
- Keep the external `MarketView` separate from the simulated matching `OrderBook`.
- Start with a seeded generator and a manually specified, versioned, length-delimited binary format; add optional normalized public crypto inputs later.
- Use exact integer ticks/lots, checked wide notional arithmetic, explicit IDs/sequences, and recorded logical time.
- Use `BTreeMap` price indexes with safe slab-indexed doubly linked FIFO orders and direct `OrderId` lookup.
- Perform risk, matching, reservations, positions, and output sequencing on the same owner thread without book mutexes.
- Make replay a primary feature: exact merged input, canonical output/state serialization, BLAKE3 digests, and periodic checkpoints.
- Use Criterion for microbenchmarks and a custom open-loop/HDR harness for threaded latency/throughput. Publish no number without raw run context.
- Keep Tokio, JSON, Node, Prometheus exposition, persistence, Solana, and Canton on cold paths.
- Use a TypeScript Fastify control process over length-prefixed local JSON IPC, with sequence-stamped read models and authenticated idempotent commands.
- Derive settlement instructions only from durable trade events, store them in a SQLite outbox, and reconcile every destination by stable settlement ID.
- Use a small Anchor collateral ledger with PDA-isolated accounts and receipt-based replay protection; restrict token behavior before claiming Token-2022 support.
- Use the deployed Canton Token Standard, preferably V2 where actually supported, for holdings/allocations/DvP; add only a small venue settlement workflow.
- Keep Solana and Canton optional and never on the matching path. A destination failure leaves the trade executed and the obligation recoverable.

### Important risks

- Tail latency can be dominated by scheduler noise, queue saturation, output amplification, allocation on price churn, clock/telemetry work, or persistence rather than matching logic.
- Asynchronous journaling has an acknowledged-tail loss window; durable acknowledgement has a latency cost. The mode must be explicit.
- Risk correctness depends on working-order reservations, exact reference-price/staleness policy, and checked multi-scale arithmetic.
- A poorly defined replace, duplicate, self-trade, or kill policy will break deterministic outputs even if the data structures are correct.
- Control projections are stale copies unless their engine sequence and age are exposed.
- Settlement retries without business-level idempotency can double-apply economics.
- Generic Token-2022 handling and broad Canton observers both create security/correctness hazards.
- Approved standards and current crate status can change; dependencies and Canton/Solana APIs need revalidation at implementation time.

### Performance hypotheses requiring benchmarks

- A specialized SPSC ring will reduce queue coordination cost relative to MPMC/general channels for this exact topology.
- A sparse `BTreeMap` plus indexed FIFO will be fast enough at realistic lab depths and simpler than a price ladder.
- Preallocated order/result storage will remove steady-state allocation from common order paths.
- Dedicated engine ownership will produce lower and more stable tails than a mutex-protected shared book.
- Adaptive spinning will offer a useful latency/CPU compromise; full busy spin may help only on an isolated host.
- Affinity and avoiding SMT siblings may reduce variance on Linux but may hurt under thermal or poor topology choices.
- Off-thread telemetry will make its engine cost small but not zero.
- Batching will increase maximum throughput while increasing queue residence at low/moderate load.
- Group commit will improve durable throughput relative to per-event sync while adding a bounded waiting component.
- A direct-index price ladder, custom decoder, alternate allocator, or unsafe code may provide no project-level benefit. None should be adopted without evidence.

### Sources

1. Binance. “[Binance Public Data](https://github.com/binance/binance-public-data/blob/master/README.md).” Public daily/monthly files, fields, timestamp note, checksums, and MIT license. Accessed 2026-09-10. [^1]
2. Coinbase Developer Platform. “[Exchange WebSocket Channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels).” Level-2 snapshots, updates, and snapshot sequence handling. Accessed 2026-09-10. [^2]
3. Zerocopy maintainers. “[zerocopy crate documentation](https://docs.rs/zerocopy/latest/zerocopy/).” Layout validation, byte conversions, and byte-order-aware numerics. Accessed 2026-09-10. [^3]
4. Bincode maintainers. “[Bincode is now unmaintained](https://docs.rs/crate/bincode/latest).” Current crate status and suggested alternatives. Accessed 2026-09-10. [^4]
5. Postcard maintainers. “[postcard crate documentation](https://docs.rs/postcard/latest/postcard/).” Stable wire-format statement and caller-provided serialization. Accessed 2026-09-10. [^5]
6. Rust Project. “[`std::collections::btree_map`](https://doc.rust-lang.org/std/collections/btree_map/).” Ordered B-tree map. Accessed 2026-09-10. [^6]
7. Tokio project. “[`slab` crate documentation](https://docs.rs/slab/latest/slab/).” Preallocation, capacity growth, vector backing, and key reuse. Accessed 2026-09-10. [^7]
8. Tokio project. “[`spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).” Dedicated-thread guidance for long-lived work. Accessed 2026-09-10. [^8]
9. rtrb maintainers. “[rtrb crate documentation](https://docs.rs/rtrb/latest/rtrb/).” Fixed-capacity SPSC behavior and allocation properties. Accessed 2026-09-10. [^9]
10. Crossbeam project. “[`ArrayQueue` source documentation](https://docs.rs/crossbeam-queue/latest/src/crossbeam_queue/array_queue.rs.html).” Bounded MPMC queue design. Accessed 2026-09-10. [^10]
11. Crossbeam project. “[`CachePadded`](https://docs.rs/crossbeam/latest/crossbeam/utils/struct.CachePadded.html).” Cache-line separation for concurrent coordination fields. Accessed 2026-09-10. [^11]
12. `core_affinity` maintainers. “[core_affinity crate documentation](https://docs.rs/core_affinity/latest/core_affinity/).” Thread-affinity API. Accessed 2026-09-10. [^12]
13. Tokio project. “[Tokio CPU-bound tasks and blocking code](https://docs.rs/tokio/latest/tokio/).” Runtime scheduling and CPU-work guidance. Accessed 2026-09-10. [^13]
14. BLAKE3 team. “[Official BLAKE3 implementation](https://github.com/BLAKE3-team/BLAKE3).” Specification-linked Rust implementation and streaming/SIMD support. Accessed 2026-09-10. [^14]
15. Criterion.rs maintainers. “[`Criterion` benchmark manager](https://docs.rs/criterion/latest/criterion/struct.Criterion.html).” Warm-up, measurement, analysis, and comparison phases. Accessed 2026-09-10. [^15]
16. HdrHistogram Rust maintainers. “[`hdrhistogram` crate](https://docs.rs/hdrhistogram/latest/hdrhistogram/).” High-dynamic-range latency histograms and correction APIs. Accessed 2026-09-10. [^16]
17. Rust Project. “[Cargo Profiles](https://doc.rust-lang.org/cargo/reference/profiles.html).” Release and bench profile behavior. Accessed 2026-09-10. [^17]
18. flamegraph-rs. “[cargo-flamegraph README](https://github.com/flamegraph-rs/flamegraph/blob/main/README.md).” Platform profiling, release benchmarks, and debug-symbol guidance. Accessed 2026-09-10. [^18]
19. proptest maintainers. “[proptest crate](https://docs.rs/proptest/latest/proptest/).” Property-based testing and shrinking. Accessed 2026-09-10. [^19]
20. Rust Fuzz project. “[Rust Fuzz Book](https://rust-fuzz.github.io/book/).” cargo-fuzz/libFuzzer and fuzzing workflow. Accessed 2026-09-10. [^20]
21. Node.js project. “[Node.js `net` IPC documentation](https://nodejs.org/api/net.html).” Unix-domain sockets and Windows named pipes. Accessed 2026-09-10. [^21]
22. Fastify project. “[Validation and Serialization](https://fastify.dev/docs/latest/Reference/Validation-and-Serialization/).” JSON Schema request/response validation. Accessed 2026-09-10. [^22]
23. Fastify project. “[`@fastify/websocket`](https://github.com/fastify/fastify-websocket).” Maintained Fastify WebSocket integration built on `ws`. Accessed 2026-09-10. [^23]
24. Prometheus Rust client maintainers. “[OpenMetrics text encoding](https://docs.rs/prometheus-client/latest/prometheus_client/encoding/text/fn.encode.html).” Registry exposition. Accessed 2026-09-10. [^24]
25. Tokio project. “[tracing crate documentation](https://docs.rs/tracing/latest/tracing/).” Structured spans/events and subscriber filtering. Accessed 2026-09-10. [^25]
26. Solana Foundation. “[Program Derived Addresses](https://solana.com/docs/core/pda).” Deterministic derivation, off-curve addresses, and program signing. Accessed 2026-09-10. [^26]
27. Anchor project. “[Account Constraints](https://www.anchor-lang.com/docs/references/account-constraints).” Signer, PDA, owner, address, mint, token, and extension constraints. Accessed 2026-09-10. [^27]
28. Anchor project. “[Transfer Tokens](https://www.anchor-lang.com/docs/tokens/basics/transfer-tokens).” Checked token transfers, token interface, and PDA authority. Accessed 2026-09-10. [^28]
29. Solana Foundation. “[Token Extensions](https://solana.com/docs/tokens/extensions).” Token-2022 extension state, behaviors, and compatibility limitations. Accessed 2026-09-10. [^29]
30. Solana Foundation. “[Writing to the Network](https://solana.com/docs/intro/quick-start/writing-to-network).” Atomic instruction execution within a transaction. Accessed 2026-09-10. [^30]
31. Solana Foundation. “[`confirmTransaction` migration](https://solana.com/docs/rpc/deprecated/confirmtransaction).” `getSignatureStatuses` confirmation guidance. Accessed 2026-09-10. [^31]
32. Solana Foundation. “[Transaction Confirmation & Expiration](https://solana.com/developers/cookbook/transactions/confirmation).” Recent blockhash expiration, lagging RPC nodes, and rebroadcast considerations. Accessed 2026-09-10. [^32]
33. Canton Foundation. “[CIP-0056: Canton Network Token Standard](https://github.com/canton-foundation/cips/blob/main/cip-0056/cip-0056.md).” Holdings, transfer, allocations, and atomic DvP APIs. Accessed 2026-09-10. [^33]
34. Canton Foundation. “[CIP-0112: Canton Network Token Standard V2](https://github.com/canton-foundation/cips/blob/main/cip-0112/cip-0112.md).” Approved v2 privacy, accounting, allocation, iterated settlement, and batching changes. Accessed 2026-09-10. [^34]
35. Digital Asset. “[Daml Templates](https://docs.digitalasset.com/build/3.4/reference/daml/templates.html).” Signatory, observer, and contract authorization semantics. Accessed 2026-09-10. [^35]
36. Digital Asset. “[Compose choices: Privacy](https://docs.digitalasset.com/build/3.4/tutorials/smart-contracts/compose.html).” Stakeholder visibility and consequence disclosure. Accessed 2026-09-10. [^36]
37. Digital Asset. “[Secure Participant Query Store](https://docs.digitalasset.com/build/3.4/component-howtos/pqs/secure.html).” Party-scoped authorization and query isolation. Accessed 2026-09-10. [^37]
38. Digital Asset. “[Ledger API Services](https://docs.digitalasset.com/build/3.4/explanations/ledger-api-services.html).” Command/change-ID deduplication behavior. Accessed 2026-09-10. [^38]
39. `thiserror` maintainers. “[thiserror crate documentation](https://docs.rs/thiserror/latest/thiserror/).” Standard error derivation and API behavior. Accessed 2026-09-10. [^39]
40. `crc32fast` maintainers. “[crc32fast crate documentation](https://docs.rs/crc32fast/latest/crc32fast/).” CRC32 implementation and runtime feature selection. Accessed 2026-09-10. [^40]
41. Serde project. “[Serde crate documentation](https://docs.rs/serde/latest/serde/).” Serialization framework and supported formats. Accessed 2026-09-10. [^41]

[^1]: [Binance Public Data](https://github.com/binance/binance-public-data/blob/master/README.md).
[^2]: [Coinbase Exchange WebSocket Channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels).
[^3]: [`zerocopy` crate documentation](https://docs.rs/zerocopy/latest/zerocopy/).
[^4]: [Current `bincode` crate maintenance notice](https://docs.rs/crate/bincode/latest).
[^5]: [`postcard` crate documentation](https://docs.rs/postcard/latest/postcard/).
[^6]: [Rust standard-library `BTreeMap` documentation](https://doc.rust-lang.org/std/collections/btree_map/).
[^7]: [`slab` crate capacity and reuse documentation](https://docs.rs/slab/latest/slab/).
[^8]: [Tokio `spawn_blocking` guidance](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).
[^9]: [`rtrb` SPSC ring-buffer documentation](https://docs.rs/rtrb/latest/rtrb/).
[^10]: [Crossbeam `ArrayQueue` documentation/source](https://docs.rs/crossbeam-queue/latest/src/crossbeam_queue/array_queue.rs.html).
[^11]: [Crossbeam `CachePadded` documentation](https://docs.rs/crossbeam/latest/crossbeam/utils/struct.CachePadded.html).
[^12]: [`core_affinity` documentation](https://docs.rs/core_affinity/latest/core_affinity/).
[^13]: [Tokio CPU-bound work documentation](https://docs.rs/tokio/latest/tokio/).
[^14]: [Official BLAKE3 implementation repository](https://github.com/BLAKE3-team/BLAKE3).
[^15]: [Criterion benchmark lifecycle documentation](https://docs.rs/criterion/latest/criterion/struct.Criterion.html).
[^16]: [Rust HdrHistogram documentation](https://docs.rs/hdrhistogram/latest/hdrhistogram/).
[^17]: [Cargo profile reference](https://doc.rust-lang.org/cargo/reference/profiles.html).
[^18]: [cargo-flamegraph documentation](https://github.com/flamegraph-rs/flamegraph/blob/main/README.md).
[^19]: [proptest documentation](https://docs.rs/proptest/latest/proptest/).
[^20]: [Rust Fuzz Book](https://rust-fuzz.github.io/book/).
[^21]: [Node.js local IPC documentation](https://nodejs.org/api/net.html).
[^22]: [Fastify validation documentation](https://fastify.dev/docs/latest/Reference/Validation-and-Serialization/).
[^23]: [Fastify WebSocket plugin](https://github.com/fastify/fastify-websocket).
[^24]: [`prometheus-client` OpenMetrics encoder documentation](https://docs.rs/prometheus-client/latest/prometheus_client/encoding/text/fn.encode.html).
[^25]: [`tracing` documentation](https://docs.rs/tracing/latest/tracing/).
[^26]: [Solana PDA documentation](https://solana.com/docs/core/pda).
[^27]: [Anchor account constraints](https://www.anchor-lang.com/docs/references/account-constraints).
[^28]: [Anchor token transfer documentation](https://www.anchor-lang.com/docs/tokens/basics/transfer-tokens).
[^29]: [Solana Token-2022 extension documentation](https://solana.com/docs/tokens/extensions).
[^30]: [Solana transaction execution documentation](https://solana.com/docs/intro/quick-start/writing-to-network).
[^31]: [Solana transaction status migration guidance](https://solana.com/docs/rpc/deprecated/confirmtransaction).
[^32]: [Solana confirmation and blockhash-expiration guidance](https://solana.com/developers/cookbook/transactions/confirmation).
[^33]: [Canton CIP-0056](https://github.com/canton-foundation/cips/blob/main/cip-0056/cip-0056.md).
[^34]: [Canton CIP-0112](https://github.com/canton-foundation/cips/blob/main/cip-0112/cip-0112.md).
[^35]: [Daml template authorization reference](https://docs.digitalasset.com/build/3.4/reference/daml/templates.html).
[^36]: [Daml privacy and disclosure tutorial](https://docs.digitalasset.com/build/3.4/tutorials/smart-contracts/compose.html).
[^37]: [Digital Asset party-scoped query security guidance](https://docs.digitalasset.com/build/3.4/component-howtos/pqs/secure.html).
[^38]: [Canton/Daml Ledger API command deduplication documentation](https://docs.digitalasset.com/build/3.4/explanations/ledger-api-services.html).
[^39]: [`thiserror` documentation](https://docs.rs/thiserror/latest/thiserror/).
[^40]: [`crc32fast` documentation](https://docs.rs/crc32fast/latest/crc32fast/).
[^41]: [Serde documentation](https://docs.rs/serde/latest/serde/).

### Ready for Implementation

- [ ] Confirm the Phase 1 venue policies: self-trade, GTC/IOC scope, replace priority, market-order reference, and duplicate handling.
- [ ] Choose initial hard capacities and define every capacity-exhaustion result.
- [ ] Freeze v1 integer scales, IDs, sequence rules, reject ordering, binary fields, and canonical checksum schema.
- [ ] Select a Rust MSRV/toolchain and recheck dependency maintenance, licenses, advisories, and minimal features.
- [ ] Choose the primary Linux benchmark host and environment-capture procedure.
- [ ] Define benchmark workload seeds/mixes and empty result templates without publishing invented values.
- [ ] Confirm asynchronous versus durable acknowledgement defaults and document the crash-loss boundary.
- [ ] Confirm Phase 2 IPC authentication, Node LTS, SQLite interface, and projection retention.
- [ ] Define deterministic settlement batch closure, manifest, exact asset scales, and netting assumptions.
- [ ] Restrict the initial Solana mint/token-program/extension policy and choose local-validator finality tests.
- [ ] Select an actual Canton environment and verify its deployed Token Standard v1/v2 packages before coding.
- [ ] Turn each invariant and failure row above into a named unit, property, fuzz, integration, or recovery test.
- [ ] Keep implementation to the two phases and preserve the hot/cold boundary.
- [ ] Delete `INVESTIGATION.md` when implementation begins, as planned.
