# ADR 0010 — Portfolio, P&L and analytics

Status: accepted (phase 8). Code: `src/trader/portfolio/`, `src/trader/analytics/`, `Broker.deal_history`,
`trader report`, `trader pnl-check`. Spec: SPEC §9, §10.

Acceptance: **P&L matches the broker statement for a demo day (documented tolerance)**. The demo test is
`tests/contract/test_pnl_demo.py`, run with `-m demo`. The same comparison runs on every test run on paper
and on the cTrader fake: `tests/integration/test_app_run.py`, and the contract test
`test_deal_history_is_the_statement` on every adapter.

## Realized P&L = broker deals

`Broker.deal_history(start, end)` (new on every adapter) returns the trade deals executed in [start, end):

| Broker | Source |
|---|---|
| paper | its own record |
| cTrader | `ProtoOADealListReq`, with the chunked-window bisect shared with order history |
| Exness | `GET …/history/accounts/{id}/deals`, types `open`/`close`; it raises rather than truncate |

A `Fill` carries the broker's own money figures:
- `realized_pnl` (gross, closing deals), `commission`, `swap`;
- `balance_after` where the broker reports it (cTrader closing deals, paper).

`Fill.net` is what a deal did to the balance: gross + commission + swap.

The **fill ledger** (`portfolio/ledger.py`, `data/portfolio/<account>/fills.jsonl`) holds every deal once,
deduplicated by deal id. Each deal is attributed to:
- a strategy: the order's `strategy_id` via the client id, else the position's owner, else `manual`;
- a trading day, using the account's `risk.day_offset_minutes`, the same day as the daily loss limit.

Live fills go in as they arrive, and **the ledger is backfilled from `deal_history` on start and after every
reconnect**. Deals done while we were disconnected therefore still count. Live and backfilled copies of a
deal collapse into one.

Paper broker ids now carry a per-instance prefix (`PDk3f9xq-12`), so they stay unique across restarts
for the ledger and the journal.

## Portfolio (`portfolio/book.py`, `portfolio/service.py`)

- **Unrealized P&L** is recomputed on every tick, at the exit side (bid for longs, ask for shorts): price move
  × volume × contract size, converted from the quote currency with the conversion pair's live tick. A
  position without a price or conversion rate is counted as *unpriced* rather than guessed.
- **Computed balance** = the broker balance at the baseline + the net of every ledger deal after it.
  **Computed equity** = computed balance + computed unrealized.
- **Equity curve** is sampled every `portfolio.equity_sample_s` (60 s) and on every fill
  (`equity-<day>.jsonl`). Each sample carries the high-water mark and drawdown. The risk engine keeps its own
  equity peak from broker equity (ADR 0008); this curve is the reporting one.
- **Drift check:**
  - `equity_drift` alert: computed equity differs from broker equity by more than
    `max(drift_min, drift_pct % of equity)`. Defaults are 1.00 and 0.5 %, and the alert fires once until the
    gap closes.
  - `balance_mismatch` alert: the broker balance moved with no matching deal. It waits `balance_grace_s`
    and for any running backfill, because deals and balance updates can arrive in either order. This is
    usually a deposit, withdrawal or fee, so the baseline is reset after the alert.

  Both are `AlertEvent`s; phase 10 delivers them as notifications.
- **Attribution:** `ledger.realized(day, strategy, symbol)` and `ledger.attribution(day)` give gross,
  commission, swap and net per (strategy, symbol, day), i.e. gross vs net after costs.

## Analytics

**Trade journal** (`analytics/journal.py`, `data/journal/<account>/trades.jsonl` + `notes.jsonl`). One
record per round trip, from a position's first fill until its volume returns to zero:

| Field | Definition |
|---|---|
| entry/exit price | volume-weighted over the entry/exit fills |
| volume | the largest open volume |
| gross / commission / swap / net | summed over the trade's deals |
| SL/TP history | from the opening order and every position update |
| MAE / MFE | the worst / best excursion from the entry while open, from exit-side ticks, in price and money |
| initial risk | distance to the opening order's SL × volume × contract size × rate |
| R multiple | net / initial risk (none without an SL) |
| version, tags, params hash | strategy version and tags from the opening order; a hash of the strategy's config params |

Notes can be added to a trade afterwards. After a restart, open trades are rebuilt by replaying the ledger;
closed ones are not duplicated. MAE/MFE for the time we were down is lost.

**Execution quality** (`analytics/execution.py`, `data/executions/<account>.jsonl`). One record per order
that was actually sent; risk rejects are excluded. It holds the signal, send (PENDING_SUBMIT), ACK and
first-fill times, the reference price, the fill price, the final status, and the retry and requote counts.
The OMS now publishes an `OrderRetryEvent` on each same-id resend, which is where requotes are counted.
- **Reference price:** the intent's signal price for MARKET orders, else the limit/stop price.
- **Slippage** is side-adjusted, in points and money: **positive = adverse**, negative = price improvement.
- **Latencies** in ms: signal→send, send→ack, ack→fill, total.
- **Stats** group by any key, by default (broker, symbol, UTC hour). They give count, fill/reject/requote
  rates, slippage mean/p50/p95 and latency p50/p95/p99 (nearest-rank), so brokers can be compared side by
  side.

**Metrics** (`analytics/metrics.py`) are computed per strategy, symbol, account or any grouping:

| Metric | Definition |
|---|---|
| win / loss | net > 0 / net < 0 |
| win rate, avg win, avg loss | from the above |
| profit factor | wins / \|losses\| |
| expectancy | average net per trade |
| avg R | over trades with an SL |
| max drawdown | on the trade-net equity curve, absolute and % from `start_equity` |
| recovery factor | net / max drawdown |
| Sharpe / Sortino | daily returns over **every weekday** in the period (no trades = 0 %), annualised × √252 |
| streaks | longest win and loss streaks |
| exposure | share of the period with a trade open |
| trades per day | over weekdays |

**Daily report** (`analytics/reports.py`, `data/reports/<account>/<day>.json` plus `-trades.csv` and
`-executions.csv`). It contains:
- realized P&L in total and per (strategy, symbol);
- the equity close, unrealized P&L, high-water mark, drawdown, and the broker's equity;
- trade metrics overall and per strategy;
- risk decisions by rule;
- execution stats overall and per (broker, symbol).

`trader run` writes it at every trading-day rollover and at shutdown (the day so far). `trader report`
rebuilds it from the stored books. Risk counts are only available live.

## `trader pnl-check` (the acceptance check)

The command connects read-only, fetches the broker's deals for the trading day, and compares them with our
ledger:
1. every deal is present on both sides (by id);
2. each deal's side, volume and price match, and gross, commission and swap agree;
3. the day's totals agree;
4. **balance chain:** where the broker reports `balance_after`, the change between two such deals equals the
   net of the deals in between.

**Tolerance:** 0.01 account currency per deal (rounding). Totals are allowed 0.01 × the number of deals.
The command exits 1 on any mismatch.

The demo test adds an independent check: the balance change over a round trip must equal the trade's net.
It also runs `test_deal_history_is_the_statement`, which checks the broker's deals against its balance.
Together these settle the documented assumptions about how each broker reports commission and swap
(`docs/brokers/*.md`).

## Consequences

- A balance operation shows as a `balance_mismatch` alert and re-baselines the computed balance. Deposits
  and withdrawals are not tracked as their own records yet.
- Exness deals carry no swap. If swap is charged as a balance-type deal, it is not a fill and appears as a
  balance mismatch; to be settled by the demo probe.
- Books are JSONL files until the phase 9 database. Writes are appended off the event loop every second
  and at shutdown.
