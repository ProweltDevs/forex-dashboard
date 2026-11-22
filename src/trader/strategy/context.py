"""StrategyContext: everything a strategy may read, and the only way it can act (SPEC §8).

Reads go through a ``StrategyEnv`` (market data + the OMS's account view): pure, synchronous, no IO.
Actions (``buy``/``sell``/``place``/``modify``/``cancel``/``close``/``modify_position``) only *queue*
commands; the runner sends them through ``OrderManager`` (and so through risk) after the callback
returns, and drops them if the callback raised.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

from pydantic import BaseModel

from trader.brokers.base import OrderChanges
from trader.domain.enums import OrderType, Side, Timeframe, TimeInForce
from trader.domain.errors import ConversionUnavailable
from trader.domain.exits import ExitSpec
from trader.domain.fx import conversion_rate
from trader.domain.jsontypes import JsonValue
from trader.domain.models import AccountState, Bar, Instrument, Order, OrderIntent, Position, Tick
from trader.observability.logging import get_logger
from trader.risk import sizing
from trader.risk.gate import AccountView

from .indicators.base import Indicator


class StrategyEnv(Protocol):
    """Read-only world view for one strategy (live: market data + OMS; later: the backtester)."""

    def now(self) -> datetime: ...
    def bars(self, symbol: str, tf: Timeframe, n: int | None = None) -> list[Bar]: ...
    def last_tick(self, symbol: str) -> Tick | None: ...
    def view(self) -> AccountView: ...


# ---------------------------------------------------------------------------------------------- commands


@dataclass(frozen=True, slots=True)
class Submit:
    intent: OrderIntent


@dataclass(frozen=True, slots=True)
class ModifyOrder:
    client_order_id: str
    changes: OrderChanges


@dataclass(frozen=True, slots=True)
class CancelOrder:
    client_order_id: str


@dataclass(frozen=True, slots=True)
class ClosePosition:
    position_id: str
    volume: Decimal | None


@dataclass(frozen=True, slots=True)
class ModifyPosition:
    position_id: str
    sl: Decimal | None
    tp: Decimal | None


type Command = Submit | ModifyOrder | CancelOrder | ClosePosition | ModifyPosition


@dataclass(slots=True)
class Timer:
    name: str
    every: timedelta | None
    next_at: datetime


@dataclass(slots=True)
class _Series:
    symbol: str
    tf: Timeframe
    indicator: Indicator[Any]
    last_start: datetime | None = None

    def feed(self, bar: Bar) -> None:
        if self.last_start is not None and bar.start <= self.last_start:
            return  # already seen (history and live bars may overlap at creation)
        self.indicator.update(bar)
        self.last_start = bar.start


class StrategyContext:
    def __init__(
        self,
        *,
        strategy_id: str,
        version: str,
        symbols: tuple[str, ...],
        timeframes: tuple[Timeframe, ...],
        params: BaseModel,
        env: StrategyEnv,
        state: dict[str, JsonValue],
        seed: int = 0,
    ) -> None:
        self.strategy_id = strategy_id
        self.version = version
        self.symbols = symbols
        self.timeframes = timeframes
        self.params = params
        self.state = state
        self.random = random.Random(seed)  # the only allowed source of randomness
        self.log = get_logger("trader.strategy").bind(strategy=strategy_id)
        self._env = env
        self._commands: list[Command] = []
        self._series: dict[tuple[object, ...], _Series] = {}
        self.timers: dict[str, Timer] = {}

    # ================================================================== reads

    def now(self) -> datetime:
        return self._env.now()

    def bars(self, symbol: str, tf: Timeframe, n: int | None = None) -> list[Bar]:
        """Complete bars, oldest first (the last ``n`` if given)."""
        self._check(symbol, tf)
        return self._env.bars(symbol, tf, n)

    def last_tick(self, symbol: str) -> Tick | None:
        self._check(symbol)
        return self._env.last_tick(symbol)

    def instrument(self, symbol: str) -> Instrument:
        self._check(symbol)
        return self._env.view().instruments[symbol]

    def account(self) -> AccountState | None:
        return self._env.view().account

    def positions(self, symbol: str | None = None) -> list[Position]:
        """This strategy's open positions."""
        return [
            p
            for p in self._env.view().positions
            if p.strategy_id == self.strategy_id and (symbol is None or p.symbol == symbol)
        ]

    def orders(self, symbol: str | None = None) -> list[Order]:
        """This strategy's open (non-terminal) orders."""
        return [
            o
            for o in self._env.view().open_orders
            if o.request.strategy_id == self.strategy_id and (symbol is None or o.request.symbol == symbol)
        ]

    def indicator[I: Indicator[Any]](
        self, kind: Callable[..., I], symbol: str, tf: Timeframe, **params: Any
    ) -> I:
        """A managed indicator on (symbol, tf): created once per arguments, warmed up from the bar history,
        then updated by the runner with every complete bar before ``on_bar``."""
        self._check(symbol, tf)
        key = (kind, symbol, tf, tuple(sorted(params.items())))
        series = self._series.get(key)
        if series is None:
            series = self._series[key] = _Series(symbol=symbol, tf=tf, indicator=kind(**params))
            for bar in self._env.bars(symbol, tf):
                series.feed(bar)
        return series.indicator  # type: ignore[return-value]

    def size_by_risk(
        self, symbol: str, *, entry: Decimal, stop: Decimal, risk_pct: Decimal
    ) -> Decimal | None:
        """Lots that lose ``risk_pct`` % of equity at ``stop`` (rounded down; None if not tradable)."""
        view, inst = self._env.view(), self.instrument(symbol)
        account = view.account
        if account is None:
            return None
        try:
            rate = conversion_rate(inst.quote_currency, account.currency, view.last_ticks.get)
        except ConversionUnavailable:
            return None
        return sizing.percent_of_equity(
            inst, equity=account.equity, risk_pct=risk_pct, entry=entry, stop=stop, quote_to_account=rate
        )

    # ================================================================== actions (queued)

    def buy(self, symbol: str, volume: Decimal, **kw: Any) -> OrderIntent:
        return self.place(Side.BUY, symbol, volume, **kw)

    def sell(self, symbol: str, volume: Decimal, **kw: Any) -> OrderIntent:
        return self.place(Side.SELL, symbol, volume, **kw)

    def place(
        self,
        side: Side,
        symbol: str,
        volume: Decimal,
        *,
        limit_price: Decimal | None = None,
        stop_price: Decimal | None = None,
        sl: Decimal | None = None,
        tp: Decimal | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        expiry: datetime | None = None,
        max_slippage: Decimal | None = None,
        exits: tuple[ExitSpec, ...] = (),
        oco_group: str | None = None,
        tags: tuple[str, ...] = (),
    ) -> OrderIntent:
        """Queue a new order. MARKET without prices, LIMIT with ``limit_price``, STOP with ``stop_price``,
        STOP_LIMIT with both. Returns the intent (the OMS assigns its client_order_id)."""
        self._check(symbol)
        if limit_price is not None and stop_price is not None:
            order_type = OrderType.STOP_LIMIT
        elif limit_price is not None:
            order_type = OrderType.LIMIT
        elif stop_price is not None:
            order_type = OrderType.STOP
        else:
            order_type = OrderType.MARKET
        tick = self._env.last_tick(symbol)
        intent = OrderIntent(
            symbol=symbol,
            side=side,
            order_type=order_type,
            volume=volume,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            signal_ts=self.now(),
            limit_price=limit_price,
            stop_price=stop_price,
            sl=sl,
            tp=tp,
            time_in_force=time_in_force,
            expiry=expiry,
            max_slippage=max_slippage,
            signal_price=None if tick is None else (tick.ask if side is Side.BUY else tick.bid),
            tags=tags,
            exits=exits,
            oco_group=oco_group,
        )
        self._commands.append(Submit(intent))
        return intent

    def modify(self, order: Order | str, changes: OrderChanges) -> None:
        self._commands.append(ModifyOrder(_cid(order), changes))

    def cancel(self, order: Order | str) -> None:
        self._commands.append(CancelOrder(_cid(order)))

    def close(self, position: Position | str, volume: Decimal | None = None) -> None:
        self._commands.append(ClosePosition(_pid(position), volume))

    def close_all(self, symbol: str | None = None) -> None:
        for p in self.positions(symbol):
            self.close(p)

    def modify_position(self, position: Position | str, *, sl: Decimal | None, tp: Decimal | None) -> None:
        self._commands.append(ModifyPosition(_pid(position), sl, tp))

    # ================================================================== timers

    def set_timer(self, name: str, *, every: timedelta | None = None, at: datetime | None = None) -> None:
        """Fire ``on_timer(name)`` at ``at`` and/or every ``every`` (replaces a timer of the same name)."""
        if every is None and at is None:
            raise ValueError("set_timer needs every= or at=")
        if every is not None and every <= timedelta(0):
            raise ValueError("every must be positive")
        first = at if at is not None else self.now() + every  # type: ignore[operator]
        self.timers[name] = Timer(name=name, every=every, next_at=first)

    def cancel_timer(self, name: str) -> None:
        self.timers.pop(name, None)

    # ================================================================== runner side

    def take_commands(self) -> list[Command]:
        commands, self._commands = self._commands, []
        return commands

    def feed_bar(self, bar: Bar) -> None:
        for series in self._series.values():
            if series.symbol == bar.symbol and series.tf is bar.timeframe:
                series.feed(bar)

    def due_timers(self, now: datetime) -> list[str]:
        """Names of timers due at ``now`` (each fires once even if several periods were missed)."""
        due = []
        for name, timer in list(self.timers.items()):
            if timer.next_at > now:
                continue
            due.append(name)
            if timer.every is None:
                del self.timers[name]
            else:
                while timer.next_at <= now:
                    timer.next_at += timer.every
        return due

    def _check(self, symbol: str, tf: Timeframe | None = None) -> None:
        if symbol not in self.symbols:
            raise ValueError(f"{self.strategy_id} is not subscribed to {symbol} (symbols: {self.symbols})")
        if tf is not None and tf not in self.timeframes:
            raise ValueError(f"{self.strategy_id} is not subscribed to {tf} (timeframes: {self.timeframes})")


def _cid(order: Order | str) -> str:
    return order if isinstance(order, str) else order.client_order_id


def _pid(position: Position | str) -> str:
    return position if isinstance(position, str) else position.position_id
