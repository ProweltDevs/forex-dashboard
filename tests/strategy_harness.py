"""Deterministic driver for strategy tests: feeds bars exactly like the runner (indicators first, then
``on_bar``) and returns the commands each callback queued. No OMS, no broker, no wall clock."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from tests.factories import eurusd
from trader.domain import Position, Side, Tick, Timeframe
from trader.domain.models import AccountState, Bar, Instrument, Order
from trader.risk.gate import AccountView
from trader.strategy.base import Strategy
from trader.strategy.context import Command, StrategyContext, Submit

D = Decimal
T0 = datetime(2026, 1, 6, 0, 0, tzinfo=UTC)


class HarnessEnv:
    def __init__(self, instruments: dict[str, Instrument], equity: Decimal) -> None:
        self.now_ts = T0
        self.instruments = instruments
        self.equity = equity
        self.history: dict[tuple[str, Timeframe], list[Bar]] = {}
        self.ticks: dict[str, Tick] = {}
        self.positions: list[Position] = []
        self.orders: list[Order] = []

    def now(self) -> datetime:
        return self.now_ts

    def bars(self, symbol: str, tf: Timeframe, n: int | None = None) -> list[Bar]:
        bars = self.history.get((symbol, tf), [])
        return bars[-n:] if n else list(bars)

    def last_tick(self, symbol: str) -> Tick | None:
        return self.ticks.get(symbol)

    def view(self) -> AccountView:
        acct = AccountState(
            account_id="h",
            currency="USD",
            balance=self.equity,
            equity=self.equity,
            margin=D(0),
            free_margin=self.equity,
            leverage=100,
            ts=self.now_ts,
        )
        return AccountView(
            account_id="h",
            account=acct,
            positions=tuple(self.positions),
            open_orders=tuple(self.orders),
            instruments=self.instruments,
            last_ticks=dict(self.ticks),
            broker_connected=True,
        )


class Harness:
    def __init__(
        self,
        cls: type[Strategy[Any]],
        params: dict[str, Any] | None = None,
        *,
        tf: Timeframe = Timeframe.M5,
        spread: str = "0.00010",
        equity: str = "10000",
    ) -> None:
        self.tf = tf
        self.spread = D(spread)
        self.env = HarnessEnv({"EURUSD": eurusd()}, D(equity))
        p = cls.Params.model_validate(params or {})
        self.strategy = cls("test", p)
        self.ctx = StrategyContext(
            strategy_id="test",
            version=cls.version,
            symbols=("EURUSD",),
            timeframes=(tf,),
            params=p,
            env=self.env,
            state={},
        )
        self._i = 0
        self._last: Decimal | None = None

    def start(self) -> list[Command]:
        self.strategy.on_start(self.ctx)
        return self.ctx.take_commands()

    def push(self, close: str | Decimal, *, wick: str = "0.00020") -> list[Command]:
        c = D(close)
        o = self._last if self._last is not None else c
        duration = self.tf.duration
        assert duration is not None
        start = T0 + duration * self._i
        bar = Bar(
            symbol="EURUSD",
            timeframe=self.tf,
            start=start,
            open=o,
            high=max(o, c) + D(wick),
            low=min(o, c) - D(wick),
            close=c,
            volume=D(10),
            complete=True,
        )
        self._i += 1
        self._last = c
        self.env.history.setdefault(("EURUSD", self.tf), []).append(bar)
        self.env.now_ts = start + duration
        self.env.ticks["EURUSD"] = Tick(
            symbol="EURUSD",
            bid=c,
            ask=c + self.spread,
            ts=self.env.now_ts,
            recv_ts=self.env.now_ts,
            recv_mono_ns=0,
        )
        self.ctx.feed_bar(bar)
        self.strategy.on_bar(self.ctx, bar)
        return self.ctx.take_commands()

    def run(self, closes: list[str]) -> list[tuple[int, Command]]:
        out = []
        for i, c in enumerate(closes):
            out.extend((i, cmd) for cmd in self.push(c))
        return out

    def fill(self, cmd: Command) -> Position:
        """Pretend the intent filled: the strategy now holds the position."""
        assert isinstance(cmd, Submit)
        i = cmd.intent
        tick = self.env.ticks[i.symbol]
        pos = Position(
            position_id=f"P{len(self.env.positions)}",
            symbol=i.symbol,
            side=i.side,
            volume=i.volume,
            entry_price=tick.ask if i.side is Side.BUY else tick.bid,
            opened_ts=self.env.now_ts,
            strategy_id="test",
            sl=i.sl,
            tp=i.tp,
        )
        self.env.positions.append(pos)
        return pos
