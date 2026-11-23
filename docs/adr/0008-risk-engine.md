# ADR 0008 — Risk engine, kill switch and circuit breakers

Status: accepted (phase 6). Code: `src/trader/risk/`, `src/trader/config/risk.py`, `src/trader/app.py`,
`trader kill` in `src/trader/cli.py`. Spec: SPEC §7, §15.

Acceptance tests:
- `tests/unit/risk/test_rules.py`: every rule has a reject case and an approve case.
- `tests/contract/test_kill_switch_demo.py`: the kill switch flattens a demo account.
- `tests/integration/test_kill_switch.py`: runs that same flow against the cTrader fake on every test run.

## Shape

There is one `RiskEngine` per account, owned by that account's `OrderManager`. Every hook runs on the
OMS writer task, synchronously and without IO:

| Hook | When it runs |
|---|---|
| `pre_trade(intent, view)` | every new order |
| `on_tick(tick)` | every tick (O(1): spread average, last tick time, clock offset) |
| `observe(event, view)` | every event the OMS publishes |
| `on_time(view)` | the OMS timer, about once a second |
| `kill(level, reason)` / `reset(reason)` | manual kill switch commands |

Risk never calls the broker. Each hook returns a `RiskOutput`: events to publish, `cancel_pending`,
`flatten`, and whether its persisted state changed. The OMS executes it. So the single-writer rule holds,
and the OMS is still the only thing that sends orders.

`risk/` may import only the domain, config, observability and `brokers/base`; an architecture test
enforces this. The margin and P&L maths moved from the paper broker to `domain/accounting.py`, so the
paper broker and risk share one implementation.

`ApproveAll` is for tests only. `trader.app.build_account`, the composition root, always builds a
`RiskEngine` from the account's `risk:` config.

## Pre-trade rules (`risk/rules.py`)

There is one pure function per SPEC §7 check. They run in a fixed order and the first rejection wins.
The rule name becomes `RiskEvent.rule` and the reason is human-readable, e.g.
`risk:loss_per_trade: loss at SL 111.00 USD > 100.00`.

| Rule | Rejects when |
|---|---|
| `instrument` | unknown symbol, or a volume off the min/max/step grid (risk never silently resizes) |
| `broker_connected` | the broker is not connected |
| `max_order_volume` | lots per order |
| `max_symbol_volume` | positions + pending orders + this order on the symbol (both sides) |
| `max_positions_*` | positions + pending entry orders, per account, symbol and strategy |
| `session` | outside the instrument's sessions, the weekend window, the daily rollover window, or a blackout (filter by symbol or currency) |
| `stale_price` | no tick, or the last tick is older than `max_tick_age_s` |
| `spread` | spread above an absolute cap in ticks, or above N × the typical spread (moving average, spikes clipped) |
| `price_sanity` | SL/TP on the wrong side of entry; a stop on the wrong side of the market; a limit/stop price more than N % from mid |
| `loss_per_trade` | no SL (unless the strategy sets `require_sl: false`), or the loss at the SL above the money cap or above N % of equity |
| `exposure` | gross/net notional above the money caps, or gross/net leverage (notional / equity) above the caps |
| `margin` | (current margin + this order's margin) / equity |
| `daily_loss` | equity is down by the daily limit since the trading-day start (realized + unrealized) |
| `drawdown` | equity is down by `max_drawdown_pct` from its peak |
| `orders_per_minute` | approved orders in the last 60 s, per account and per strategy (runaway-loop guard) |
| `order_to_fill` | orders / fills in the window, once the window holds at least `min_orders` |
| `duplicate_signal` | same strategy + symbol + side within the window |

Definitions:
- **Entry**: the limit price (LIMIT, STOP_LIMIT), the stop price (STOP), or the ask/bid (MARKET).
- **Exposure**: positions plus this order. Pending orders count toward the position count and symbol
  volume, but not toward exposure or margin, because they may never fill.

**Fail closed.** If the rule needs data that is missing, the order is rejected rather than estimated:
- no account state;
- no tick;
- no conversion rate to the account currency (e.g. EURGBP on a USD account with no GBPUSD tick).

The OMS only becomes ready after a reconcile that loads the account, so this happens only in degraded
states.

**Configuration.** Limits are set per account. Per-strategy overrides change only the fields they set
(`model_fields_set`), and `null` switches a rule off. The defaults are conservative and spelled out in
`config/live.yaml.example`; a test keeps that file equal to the code defaults, so changing a default is
visible in review.

## Sizing (`risk/sizing.py`)

Methods: fixed lots, fixed money risk, % of equity at the SL distance, and ATR-based (the stop sits
k × ATR away). The size is capped at max volume and rounded **down** to the volume step. Below the minimum
the result is `None`, never the minimum, because rounding up would risk more than asked.

Hypothesis tests check two properties:
- the loss at the stop never exceeds the risk budget, and is within one step of it;
- more risk never gives a smaller size.

Strategies will call these through their context in phase 7.

## Kill switch and circuit breakers (`risk/engine.py`)

Levels, from lowest to highest; each includes the ones below it:

| Level | Effect |
|---|---|
| PAUSE_NEW | every new intent is rejected (`risk:kill_switch`) |
| CANCEL_PENDING | also cancels every pending order |
| FLATTEN_ALL | also closes every position on the account, manual ones included |

Closes and cancels are always allowed, because they only reduce risk.

The level is the highest among the active **triggers**:

| Trigger | Default level | Clears |
|---|---|---|
| `manual` (`kill_switch` / `trader kill`) | as given | `reset` |
| `daily_loss` | FLATTEN_ALL | the next trading day (`day_offset_minutes` sets the day start), or `reset` |
| `drawdown` | FLATTEN_ALL | `reset`, which re-bases the peak to the current equity |
| `rejects` (N broker rejects in a row; risk rejects don't count) | PAUSE_NEW | `reset` |
| `reconcile` (configured kinds: `position_corrected`, `order_abandoned`) | PAUSE_NEW | `reset` |
| `feed_stale` (a traded symbol silent for T while its market is open; weekends excluded) | PAUSE_NEW | ticks resume |
| `disconnected` (longer than T) | PAUSE_NEW (cancels can't reach a disconnected broker) | reconnect |
| `clock_drift` (median of receive time − broker tick timestamp) | PAUSE_NEW | back in range |

Conditions that still hold re-trigger after a `reset`. For example, the daily loss limit stays in force
for the rest of the day.

**Enforcement.**
- The cancel/flatten actions go out immediately on escalation.
- While anything is left, they are re-sent every 5 s. This covers a pending order that filled after the
  cancel, and closes that failed.
- Every level change publishes a `RiskStateEvent` (level, previous, reason, active triggers).
- The phase 7 strategy runner will stop strategies from entering at PAUSE_NEW and above.

**Persistence.** The engine's state is saved inside the account's OMS snapshot (`OmsState.risk`), written
atomically after every change. It covers:
- the triggers;
- the trading-day start equity;
- the equity peak;
- the reject count.

So a restart cannot clear a tripped breaker or reset the daily loss baseline.

## `trader kill`

`trader kill --config <file> --account <id> (--level LEVEL | --reset) --reason <text>` runs the
production path (`build_account` → broker + RiskEngine + OMS):
1. connect and reconcile;
2. apply the level;
3. wait until pending orders are cancelled and positions closed (`--timeout`);
4. persist and disconnect.

It exits 0 when done, 1 on timeout, and 2 on config or broker errors. Live accounts need `TRADER_MODE=live`
and `--confirm-live`, the same guard as `trader run`. Paper accounts are refused: their state lives only
inside the trading process.

It is the emergency tool for when `trader run` is **not** running. While it runs, use its API
(`POST /risk/kill`) or Telegram (`/kill`). The two processes must never both write one account's snapshot:
since phase 12 the OMS state file is locked (`flock`) by whichever process owns it, so `trader kill` against
a running process is refused with that advice instead of being silently undone.

## Consequences and limits

- Daily loss and drawdown are measured on equity, so a deposit or withdrawal moves them.
  Balance-operation tracking arrives with portfolio accounting (phase 8).
- Exposure and margin estimates use our own formula (`domain/accounting.margin_required`); the broker
  remains the final check.
- **Modifications are risk-checked (phase 12).** `RiskGate.pre_modify` sees a pending order as it would be
  after the change; a change that adds risk (more volume, a new entry price, a stop removed or moved away)
  goes through the kill switch and every pre-trade rule except the rate rules (a modification is not a new
  order), with the order's own exposure replaced, not added. `pre_modify_position` checks a removed or
  widened position stop against the kill switch, `require_sl` and the loss per trade from the current
  price. Risk-reducing changes (tightening, trailing, break-even, cutting volume, adding a stop) always
  pass, also under a kill switch. OMS-generated exits (trailing, break-even) only tighten and bypass the
  check by design.
- Risk hooks run on the OMS writer, so a slow rule would delay order handling. Every rule is O(positions
  + orders).
