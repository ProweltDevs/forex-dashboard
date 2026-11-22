"""AccountRuntime (SPEC §2, §15): one broker adapter + one OrderManager (+ the shared market data service).

Startup: restore OMS state -> connect -> instruments -> start the event pump -> subscribe ticks -> the
adapter's ConnectionEvent(UP) makes the OMS reconcile -> ``ready`` -> warm up bars. ``trader run`` starts
strategies only after this. ``ready`` is cleared on every disconnect and set again after the post-reconnect
reconcile; meanwhile the OMS refuses new orders (the runner reports them as ``command_failed``) and, past
``breakers.disconnected_s``, the risk breaker pauses the account's strategies via the kill switch.

Market data is keyed by the account id (one bar/tick feed per account, also in the tick recorder).

The pump is the only consumer of ``broker.events()``. Ticks go to market data and the emulation engine
synchronously; everything else is queued to the OMS writer.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence
from datetime import timedelta

from trader.brokers.base import Broker
from trader.domain.errors import BrokerUnavailable
from trader.domain.events import TickEvent
from trader.marketdata.service import MarketDataService
from trader.observability.logging import get_logger

from .manager import OrderManager

_log = get_logger("trader.runtime")


class AccountRuntime:
    def __init__(
        self,
        account_id: str,
        broker: Broker,
        oms: OrderManager,
        *,
        market_data: MarketDataService | None = None,
        symbols: Sequence[str] = (),
        ready_timeout: timedelta = timedelta(seconds=60),
        warmup_bars: int | None = None,
    ) -> None:
        self.account_id = account_id
        self.broker = broker
        self.oms = oms
        self.market_data = market_data
        self.symbols = tuple(symbols)
        self._ready_timeout = ready_timeout
        self.warmup_bars = warmup_bars  # None = MarketDataConfig.warmup_bars
        self._pump: asyncio.Task[None] | None = None

    @property
    def ready(self) -> asyncio.Event:
        return self.oms.ready

    async def start(self, *, warm_up: bool = True) -> None:
        await self.oms.start()
        await self.broker.connect()
        instruments = await self.broker.instruments()
        self.oms.set_instruments(instruments)
        if self.market_data is not None:
            self.market_data.add_instruments(self.account_id, instruments)
        self._pump = asyncio.create_task(self._run_pump(), name=f"pump-{self.account_id}")
        if self.symbols:
            await self.broker.subscribe_ticks(self.symbols)
        try:
            await asyncio.wait_for(self.oms.ready.wait(), self._ready_timeout.total_seconds())
        except TimeoutError as exc:
            raise BrokerUnavailable(
                f"{self.account_id}: no successful reconcile within {self._ready_timeout}"
            ) from exc
        if self.market_data is not None and warm_up:
            for symbol in self.symbols:
                await self.market_data.warm_up(self.account_id, self.broker, symbol, self.warmup_bars)
        _log.info(
            "account_ready", account=self.account_id, broker=self.broker.name, symbols=list(self.symbols)
        )

    async def _run_pump(self) -> None:
        md = self.market_data
        async for event in self.broker.events():
            if isinstance(event, TickEvent):
                if md is not None:
                    md.on_tick(self.account_id, event.tick)  # one feed per account
                self.oms.on_tick(event.tick)
            else:
                self.oms.on_broker_event(event)

    async def abort(self) -> None:
        """Test hook: die like a crashed process. The OMS stops without draining or persisting, and the
        pump stops. The broker connection is not closed (a dead process sends nothing)."""
        await self.oms.abort()
        if self._pump is not None:
            self._pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pump
            self._pump = None

    async def stop(self) -> None:
        """Stop the OMS (drain + persist), then the pump and the broker connection."""
        await self.oms.stop()
        if self._pump is not None:
            self._pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pump
            self._pump = None
        await self.broker.close()
