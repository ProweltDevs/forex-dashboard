"""Pre-trade rules (SPEC §7). Each rule is a pure function of a ``RuleInput``: it returns a rejection reason
or ``None``. The engine runs them in ``RULES`` order and the first rejection wins. Missing data fails closed:
no account state, no price or no conversion rate rejects instead of guessing.

Definitions (documented in ADR 0008):
- *entry*: the limit price (LIMIT, STOP_LIMIT), the stop price (STOP), or the ask/bid (MARKET).
- *potential positions*: open positions + working orders (an entry order may become a position).
- *exposure*: notional (units x price) converted to the account currency, positions + this order. Gross =
  sum of |notional|, net = |sum of signed notional|.
- *margin usage*: (broker-reported margin + this order's margin) / equity.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal

from trader.config.risk import RiskLimits, parse_daily, parse_weekly
from trader.domain.accounting import loss_at_stop, margin_required, notional_quote
from trader.domain.enums import OrderType, Side
from trader.domain.errors import ConversionUnavailable
from trader.domain.fx import conversion_rate
from trader.domain.models import Instrument, OrderIntent, Tick
from trader.risk.gate import AccountView

ZERO = Decimal(0)
HUNDRED = Decimal(100)


@dataclass(frozen=True, slots=True, kw_only=True)
class FlowStats:
    """Recent order flow, kept by the engine."""

    orders_last_minute: int  # approved orders, whole account
    strategy_orders_last_minute: int  # approved orders of this intent's strategy
    window_orders: int  # approved orders in the order-to-fill window
    window_fills: int  # fills in the same window
    last_same_signal: datetime | None  # last approved intent with the same strategy, symbol and side


@dataclass(frozen=True, slots=True, kw_only=True)
class RuleInput:
    intent: OrderIntent
    view: AccountView
    limits: RiskLimits
    now: datetime
    flow: FlowStats
    typical_spread: Decimal | None  # None until enough ticks were seen
    day_start_equity: Decimal | None
    peak_equity: Decimal | None

    @property
    def inst(self) -> Instrument:
        return self.view.instruments[self.intent.symbol]

    @property
    def tick(self) -> Tick | None:
        return self.view.last_ticks.get(self.intent.symbol)

    def entry(self) -> Decimal | None:
        i = self.intent
        if i.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
            return i.limit_price
        if i.order_type is OrderType.STOP:
            return i.stop_price
        tick = self.tick
        if tick is None:
            return None
        return tick.ask if i.side is Side.BUY else tick.bid

    def rate(self, ccy: str) -> Decimal | None:
        """Multiplier from ``ccy`` to the account currency, from live ticks."""
        account = self.view.account
        if account is None:
            return None
        try:
            return conversion_rate(ccy, account.currency, self.view.last_ticks.get)
        except ConversionUnavailable:
            return None


type Rule = Callable[[RuleInput], str | None]


def _pct(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}%"


def _money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


# ------------------------------------------------------------------------------------- structure


def check_instrument(r: RuleInput) -> str | None:
    if r.intent.symbol not in r.view.instruments:
        return f"unknown instrument {r.intent.symbol}"
    if not r.inst.is_valid_volume(r.intent.volume):
        inst = r.inst
        return (
            f"volume {r.intent.volume} is not a valid size (min {inst.min_volume}, max {inst.max_volume}, "
            f"step {inst.volume_step})"
        )
    return None


def check_connected(r: RuleInput) -> str | None:
    return None if r.view.broker_connected else "broker is not connected"


def check_max_order_volume(r: RuleInput) -> str | None:
    cap = r.limits.max_order_volume
    if cap is not None and r.intent.volume > cap:
        return f"order volume {r.intent.volume} > {cap} lots"
    return None


def check_max_symbol_volume(r: RuleInput) -> str | None:
    cap = r.limits.max_symbol_volume
    if cap is None:
        return None
    sym = r.intent.symbol
    held = sum((p.volume for p in r.view.positions if p.symbol == sym), ZERO)
    working = sum((o.remaining_volume for o in r.view.open_orders if o.request.symbol == sym), ZERO)
    total = held + working + r.intent.volume
    if total > cap:
        parts = f"positions {held} + orders {working} + {r.intent.volume}"
        return f"{sym} volume would be {total} lots ({parts}) > {cap}"
    return None


def _potential(r: RuleInput, keep: Callable[[str, str], bool]) -> int:
    """Positions + working orders matching ``keep(symbol, strategy_id)``."""
    n = sum(1 for p in r.view.positions if keep(p.symbol, p.strategy_id))
    return n + sum(1 for o in r.view.open_orders if keep(o.request.symbol, o.request.strategy_id))


def check_max_positions_account(r: RuleInput) -> str | None:
    cap = r.limits.max_positions_account
    if cap is not None and (n := _potential(r, lambda _s, _id: True)) >= cap:
        return f"{n} open positions/orders on the account (max {cap})"
    return None


def check_max_positions_symbol(r: RuleInput) -> str | None:
    cap, sym = r.limits.max_positions_symbol, r.intent.symbol
    if cap is not None and (n := _potential(r, lambda s, _id: s == sym)) >= cap:
        return f"{n} open positions/orders on {sym} (max {cap})"
    return None


def check_max_positions_strategy(r: RuleInput) -> str | None:
    cap, sid = r.limits.max_positions_strategy, r.intent.strategy_id
    if cap is not None and (n := _potential(r, lambda _s, i: i == sid)) >= cap:
        return f"{n} open positions/orders for strategy {sid} (max {cap})"
    return None


# ------------------------------------------------------------------------------------- market


def check_stale_price(r: RuleInput) -> str | None:
    tick = r.tick
    if tick is None:
        return f"no price for {r.intent.symbol}"
    limit = r.limits.max_tick_age_s
    if limit is not None:
        age = r.now - tick.recv_ts
        if age > timedelta(seconds=float(limit)):
            return f"last {r.intent.symbol} tick is {age.total_seconds():.1f}s old (max {limit}s)"
    return None


def check_spread(r: RuleInput) -> str | None:
    tick = r.tick
    if tick is None:
        return f"no price for {r.intent.symbol}"
    spread = tick.ask - tick.bid
    points = spread / r.inst.tick_size
    cap = r.limits.max_spread_points.get(r.intent.symbol)
    if cap is not None and points > cap:
        return f"spread {points:.1f} points > {cap}"
    multiple, typical = r.limits.max_spread_multiple, r.typical_spread
    if multiple is not None and typical is not None and typical > 0 and spread > typical * multiple:
        return f"spread {spread} is {(spread / typical):.1f}x the typical {typical:.6f} (max {multiple}x)"
    return None


def check_price_sanity(r: RuleInput) -> str | None:
    i, tick = r.intent, r.tick
    entry = r.entry()
    if entry is None or tick is None:
        return f"no price for {i.symbol}"
    buy = i.side is Side.BUY
    if i.sl is not None and not (i.sl < entry if buy else i.sl > entry):
        return f"SL {i.sl} is on the wrong side of entry {entry} for a {i.side}"
    if i.tp is not None and not (i.tp > entry if buy else i.tp < entry):
        return f"TP {i.tp} is on the wrong side of entry {entry} for a {i.side}"
    if i.order_type in (OrderType.STOP, OrderType.STOP_LIMIT):
        stop = i.stop_price
        assert stop is not None
        if (buy and stop <= tick.ask) or (not buy and stop >= tick.bid):
            return f"{i.side} stop {stop} is on the wrong side of the market ({tick.bid}/{tick.ask})"
    limit = r.limits.max_price_distance_pct
    if limit is not None and i.order_type is not OrderType.MARKET:
        mid = tick.mid
        for name, price in (("limit", i.limit_price), ("stop", i.stop_price)):
            distance = abs(price - mid) / mid * HUNDRED if price is not None else ZERO
            if distance > limit:
                return f"{name} price {price} is {_pct(distance)} from the market (max {limit}%)"
    return None


# ------------------------------------------------------------------------------------- money


def check_loss_per_trade(r: RuleInput) -> str | None:
    i, lim = r.intent, r.limits
    if i.sl is None:
        return (
            "an SL is required (set require_sl: false for this strategy to allow none)"
            if lim.require_sl
            else None
        )
    if lim.max_loss_per_trade is None and lim.max_loss_per_trade_pct is None:
        return None
    entry, account, rate = r.entry(), r.view.account, r.rate(r.inst.quote_currency)
    if entry is None or account is None or rate is None:
        return "cannot value the loss at SL (no price, account state or conversion rate)"
    loss = loss_at_stop(i.side, i.volume, entry, i.sl, r.inst) * rate
    caps = [
        c for c in (lim.max_loss_per_trade, _of(lim.max_loss_per_trade_pct, account.equity)) if c is not None
    ]
    cap = min(caps)
    if loss > cap:
        return f"loss at SL {_money(loss)} {account.currency} > {_money(cap)}"
    return None


def _of(pct: Decimal | None, base: Decimal) -> Decimal | None:
    return None if pct is None else base * pct / HUNDRED


def _exposure(r: RuleInput) -> tuple[Decimal, Decimal] | None:
    """(gross, net) notional in the account currency after this order, or None if it cannot be valued."""
    gross = net = ZERO
    entry = r.entry()
    if entry is None:
        return None
    legs: list[tuple[str, Side, Decimal, Decimal | None]] = [
        (p.symbol, p.side, p.volume, None) for p in r.view.positions
    ]
    legs.append((r.intent.symbol, r.intent.side, r.intent.volume, entry))
    for symbol, side, volume, price in legs:
        inst = r.view.instruments.get(symbol)
        tick = r.view.last_ticks.get(symbol)
        rate = r.rate(inst.quote_currency) if inst is not None else None
        px = price if price is not None else (tick.mid if tick is not None else None)
        if inst is None or rate is None or px is None:
            return None
        value = notional_quote(inst, volume, px) * rate
        gross += value
        net += side.sign * value
    return gross, abs(net)


def check_exposure(r: RuleInput) -> str | None:
    lim, account = r.limits, r.view.account
    caps = (lim.max_gross_exposure, lim.max_net_exposure, lim.max_gross_leverage, lim.max_net_leverage)
    if all(c is None for c in caps):
        return None
    exposure = _exposure(r)
    if exposure is None or account is None:
        return "cannot value exposure (missing price, account state or conversion rate)"
    gross, net = exposure
    if lim.max_gross_exposure is not None and gross > lim.max_gross_exposure:
        return f"gross exposure {_money(gross)} > {lim.max_gross_exposure}"
    if lim.max_net_exposure is not None and net > lim.max_net_exposure:
        return f"net exposure {_money(net)} > {lim.max_net_exposure}"
    if account.equity <= 0:
        return "equity is not positive"
    if lim.max_gross_leverage is not None and gross / account.equity > lim.max_gross_leverage:
        return f"gross leverage {(gross / account.equity):.2f}x > {lim.max_gross_leverage}x"
    if lim.max_net_leverage is not None and net / account.equity > lim.max_net_leverage:
        return f"net leverage {(net / account.equity):.2f}x > {lim.max_net_leverage}x"
    return None


def check_margin(r: RuleInput) -> str | None:
    cap, account = r.limits.max_margin_usage_pct, r.view.account
    if cap is None:
        return None
    entry = r.entry()
    if account is None or entry is None:
        return "cannot estimate margin (no account state or price)"
    amount, ccy = margin_required(r.inst, r.intent.volume, entry, account.leverage)
    rate = r.rate(ccy)
    if rate is None:
        return f"cannot convert margin from {ccy}"
    if account.equity <= 0:
        return "equity is not positive"
    usage = (account.margin + amount * rate) / account.equity * HUNDRED
    if usage > cap:
        return f"margin usage after the trade {_pct(usage)} > {cap}%"
    return None


def check_daily_loss(r: RuleInput) -> str | None:
    return daily_loss_breach(r.limits, r.view, r.day_start_equity)


def check_drawdown(r: RuleInput) -> str | None:
    return drawdown_breach(r.limits, r.view, r.peak_equity)


def daily_loss_breach(limits: RiskLimits, view: AccountView, day_start: Decimal | None) -> str | None:
    """Shared with the circuit breaker. Realized + unrealized = the equity change since the day start."""
    caps = [c for c in (limits.daily_loss, _of(limits.daily_loss_pct, day_start or ZERO)) if c is not None]
    if not caps:
        return None
    account = view.account
    if account is None or day_start is None:
        return "daily loss unknown (no account state yet)"
    loss = day_start - account.equity
    cap = min(caps)
    if loss >= cap:
        return f"daily loss {_money(loss)} {account.currency} >= limit {_money(cap)}"
    return None


def drawdown_breach(limits: RiskLimits, view: AccountView, peak: Decimal | None) -> str | None:
    cap, account = limits.max_drawdown_pct, view.account
    if cap is None:
        return None
    if account is None or peak is None or peak <= 0:
        return "drawdown unknown (no account state yet)"
    dd = (peak - account.equity) / peak * HUNDRED
    if dd >= cap:
        return f"drawdown {_pct(dd)} from the equity peak {_money(peak)} >= {cap}%"
    return None


# ------------------------------------------------------------------------------------- flow


def check_orders_per_minute(r: RuleInput) -> str | None:
    lim, flow = r.limits, r.flow
    if lim.max_orders_per_minute is not None and flow.orders_last_minute >= lim.max_orders_per_minute:
        return f"{flow.orders_last_minute} orders in the last minute (max {lim.max_orders_per_minute})"
    cap = lim.max_orders_per_minute_strategy
    if cap is not None and flow.strategy_orders_last_minute >= cap:
        n, sid = flow.strategy_orders_last_minute, r.intent.strategy_id
        return f"{n} orders from {sid} in the last minute (max {cap})"
    return None


def check_order_to_fill(r: RuleInput) -> str | None:
    lim, flow = r.limits, r.flow
    cap = lim.max_order_to_fill_ratio
    if cap is None or flow.window_orders < lim.order_to_fill_min_orders:
        return None
    if flow.window_fills == 0 or Decimal(flow.window_orders) / flow.window_fills > cap:
        window = lim.order_to_fill_window_min
        return f"{flow.window_orders} orders for {flow.window_fills} fills in {window} min (max ratio {cap})"
    return None


def check_duplicate(r: RuleInput) -> str | None:
    window, last = r.limits.duplicate_window_s, r.flow.last_same_signal
    if window is not None and last is not None and r.now - last < timedelta(seconds=float(window)):
        i = r.intent
        return (
            f"duplicate signal: {i.strategy_id} {i.side} {i.symbol} {(r.now - last).total_seconds():.1f}s ago"
        )
    return None


# ------------------------------------------------------------------------------------- sessions


def _in_daily(now: datetime, start: time, end: time) -> bool:
    t = now.time()
    return start <= t < end if start < end else (t >= start or t < end)


def _minute_of_week(weekday: int, t: time) -> int:
    return weekday * 1440 + t.hour * 60 + t.minute


def in_weekend(now: datetime, weekend: tuple[str, str]) -> bool:
    (wd0, t0), (wd1, t1) = parse_weekly(weekend[0]), parse_weekly(weekend[1])
    m, a, b = _minute_of_week(now.weekday(), now.time()), _minute_of_week(wd0, t0), _minute_of_week(wd1, t1)
    return a <= m < b if a < b else (m >= a or m < b)


def check_session(r: RuleInput) -> str | None:
    rules, now, inst = r.limits.sessions, r.now, r.inst
    if rules.use_instrument_sessions and not inst.is_open(now):
        return f"{inst.symbol} is outside its trading sessions"
    if rules.weekend is not None and in_weekend(now, rules.weekend):
        return f"weekend window {rules.weekend[0]} - {rules.weekend[1]} UTC"
    if rules.rollover is not None and _in_daily(
        now, parse_daily(rules.rollover[0]), parse_daily(rules.rollover[1])
    ):
        return f"rollover window {rules.rollover[0]}-{rules.rollover[1]} UTC"
    for b in rules.blackouts:
        if b.start <= now < b.end and _matches(inst, b.symbols, b.currencies):
            return f"blackout until {b.end.isoformat()}: {b.reason}"
    return None


def _matches(inst: Instrument, symbols: tuple[str, ...], currencies: tuple[str, ...]) -> bool:
    if not symbols and not currencies:
        return True
    return inst.symbol in symbols or inst.base_currency in currencies or inst.quote_currency in currencies


# The order only decides which reason is reported first; every rule must pass.
RULES: Mapping[str, Rule] = {
    "instrument": check_instrument,
    "broker_connected": check_connected,
    "max_order_volume": check_max_order_volume,
    "max_symbol_volume": check_max_symbol_volume,
    "max_positions_account": check_max_positions_account,
    "max_positions_symbol": check_max_positions_symbol,
    "max_positions_strategy": check_max_positions_strategy,
    "session": check_session,
    "stale_price": check_stale_price,
    "spread": check_spread,
    "price_sanity": check_price_sanity,
    "loss_per_trade": check_loss_per_trade,
    "exposure": check_exposure,
    "margin": check_margin,
    "daily_loss": check_daily_loss,
    "drawdown": check_drawdown,
    "orders_per_minute": check_orders_per_minute,
    "order_to_fill": check_order_to_fill,
    "duplicate_signal": check_duplicate,
}
