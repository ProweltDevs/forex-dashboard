# ADR 0002 — Domain model and order state machine

Status: accepted (phase 0)

## Domain model

- Frozen `dataclass(slots=True, kw_only=True)`; `Decimal` for money/prices/volumes (validated at runtime in
  `__post_init__` so a float from an adapter fails loudly); aware UTC datetimes (naive rejected).
  `Tick` skips runtime validation because it is on the hot path; adapters construct it.
- **`OrderIntent` vs `OrderRequest`.** A strategy emits an `OrderIntent` (no ID, carries `signal_ts`/`signal_price`
  for execution-quality analytics). After risk approval the OMS calls `intent.to_request(client_order_id, volume=…)`,
  assigning the ULID and the sized/rounded volume. Adapters only ever see `OrderRequest`. Strategies cannot create
  IDs, which keeps them deterministic (ULIDs contain wall-clock time and randomness).
- **Price fields:** `limit_price` (LIMIT, STOP_LIMIT) and `stop_price` (trigger for STOP, STOP_LIMIT), validated
  per order type, instead of a single overloaded `price`. `entry_price` returns whichever applies.
- `GTD` ⇔ `expiry` set. MARKET orders cannot be `GTD`/`DAY`.
- `Instrument.normalize_volume` caps at max, rounds **down** to the step and raises `InvalidVolume` below the
  minimum; `round_price` snaps to the tick grid and `digits`.
- `RateLimited.retry_after_ms` is an int (no floats in the domain).
- Price sanity (SL/TP on the correct side, distance from market) is a **risk** rule (SPEC §7), not model validation.

## State machine (`oms/state_machine.py`)

Implements SPEC §3, with these edges made explicit:

| Edge | Why |
|---|---|
| `ACCEPTED → FILLED`, `WORKING → FILLED` | a market/pending order filled in one execution |
| `ACCEPTED → PARTIALLY_FILLED` | first partial of a market order |
| `PARTIALLY_FILLED → PARTIALLY_FILLED` | further partials |
| `ACCEPTED/WORKING/PARTIALLY_FILLED → UNKNOWN` | a modify/cancel request times out; the order's real state must be reconciled |
| `UNKNOWN → ACCEPTED/WORKING/PARTIALLY_FILLED/FILLED/CANCELLED/REJECTED/EXPIRED` | reconcile resolves to the broker's real state |
| `NEW → CANCELLED/REJECTED/EXPIRED` | "any non-terminal" per spec |

Not allowed: `PENDING_SUBMIT → FILLED` directly. Brokers with a synchronous result still produce an ACK first, so
the OMS records `ACCEPTED` then the fill (keeps send→ack→fill latencies measurable).

`PARTIALLY_FILLED`/`FILLED` are reachable only through `apply_fill()`, which keeps `filled_volume` and the VWAP
`avg_fill_price` consistent and rejects overfills. Timestamps may not go backwards. Terminal states are absorbing.
Tests cover every (from, to) pair against an independently written table, plus hypothesis random walks and VWAP.
