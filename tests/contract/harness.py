"""Adapter-neutral harness for the broker contract suite (docs/SPEC.md §4).

Each adapter provides a ``ContractHarness``: a connected, reconciled broker, a symbol to trade, and hooks
for the few things the Broker protocol cannot express (forcing a disconnect). Real adapters (phases 2-3)
add a harness factory here and are run with ``-m demo`` against DEMO accounts only.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TypeVar

import pytest
from pydantic import SecretStr

from trader.brokers.base import Broker, RateLimit
from trader.brokers.ctrader import CTraderBroker, CTraderSettings, TokenSet, TokenStore
from trader.brokers.exness import ExnessBroker, ExnessSettings
from trader.brokers.paper import FakeTickSource, PaperBroker, PaperSettings
from trader.config.secrets import read_env_file
from trader.domain import ConnectionEvent, ConnectionState, Instrument, OrderType, Side, Tick, TickEvent
from trader.domain.events import DomainEvent, FillEvent
from trader.domain.ids import new_client_order_id
from trader.domain.models import OrderRequest
from trader.domain.time import SystemClock

E = TypeVar("E")
TIMEOUT_S = 5.0
CONTRACT_STRATEGY = "contract-suite"


class EventRecorder:
    """Consumes ``broker.events()`` in the background and lets tests wait for specific events."""

    def __init__(self, broker: Broker) -> None:
        self.events: list[DomainEvent] = []
        self._changed = asyncio.Condition()
        self._task = asyncio.create_task(self._run(broker))

    async def _run(self, broker: Broker) -> None:
        async for event in broker.events():
            async with self._changed:
                self.events.append(event)
                self._changed.notify_all()

    async def wait_for(
        self,
        kind: type[E],
        predicate: Callable[[E], bool] = lambda _: True,
        *,
        after: int = 0,
        timeout: float = TIMEOUT_S,
    ) -> E:
        """Return the first event of ``kind`` (at index >= ``after``) matching ``predicate``."""

        def find() -> E | None:
            for event in self.events[after:]:
                if isinstance(event, kind) and predicate(event):
                    return event
            return None

        async with asyncio.timeout(timeout), self._changed:
            while (found := find()) is None:
                await self._changed.wait()
            return found

    def of(self, kind: type[E], after: int = 0) -> list[E]:
        return [e for e in self.events[after:] if isinstance(e, kind)]

    @property
    def mark(self) -> int:
        return len(self.events)

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


@dataclass
class ContractHarness:
    broker: Broker
    symbol: str
    recorder: EventRecorder
    force_disconnect: Callable[[], Awaitable[None]]
    reconnect: Callable[[], Awaitable[None]]
    teardown: Callable[[], Awaitable[None]]
    placed: list[str] = field(default_factory=list)  # client_order_ids sent by the test

    async def instrument(self) -> Instrument:
        return next(i for i in await self.broker.instruments() if i.symbol == self.symbol)

    async def last_tick(self) -> Tick:
        event = await self.recorder.wait_for(TickEvent, lambda e: e.tick.symbol == self.symbol)
        ticks = [e for e in self.recorder.of(TickEvent) if e.tick.symbol == self.symbol]
        return ticks[-1].tick if ticks else event.tick

    def request(self, side: Side, volume: Decimal, **kwargs: object) -> OrderRequest:
        cid = new_client_order_id()
        self.placed.append(cid)
        fields: dict[str, object] = {
            "client_order_id": cid,
            "symbol": self.symbol,
            "side": side,
            "order_type": OrderType.MARKET,
            "volume": volume,
            "strategy_id": CONTRACT_STRATEGY,
            "strategy_version": "1",
            "signal_ts": datetime.now(UTC),
        }
        fields.update(kwargs)
        return OrderRequest(**fields)  # type: ignore[arg-type]

    async def fill_for(self, client_order_id: str, after: int = 0) -> FillEvent:
        return await self.recorder.wait_for(
            FillEvent, lambda e: e.fill.client_order_id == client_order_id, after=after
        )

    async def cleanup(self) -> None:
        """Leave the account as we found it: cancel our orders and close positions our orders opened."""
        broker = self.broker
        with contextlib.suppress(Exception):
            ours = set(self.placed)
            for order in await broker.open_orders():
                if order.client_order_id in ours and order.broker_order_id:
                    await broker.cancel_order(order.broker_order_id)
            opened = {
                e.fill.position_id for e in self.recorder.of(FillEvent) if e.fill.client_order_id in ours
            }
            for pos in await broker.positions():
                if pos.position_id in opened:
                    await broker.close_position(pos.position_id)
        await self.recorder.stop()
        await self.teardown()


# ---------------------------------------------------------------------------------------------- paper


def paper_instruments() -> list[Instrument]:
    return [
        Instrument(
            symbol="EURUSD",
            broker_symbol="EURUSD",
            broker_id="1",
            digits=5,
            tick_size=Decimal("0.00001"),
            contract_size=Decimal(100_000),
            min_volume=Decimal("0.01"),
            max_volume=Decimal(50),
            volume_step=Decimal("0.01"),
            base_currency="EUR",
            quote_currency="USD",
            margin_currency="EUR",
        ),
    ]


async def paper_harness() -> ContractHarness:
    clock = SystemClock()
    source = FakeTickSource(paper_instruments(), clock, base_prices={"EURUSD": Decimal("1.10000")})
    broker = PaperBroker(
        source,
        PaperSettings(
            account_id="contract-paper",
            rate_limit=RateLimit(name="requests", rate_per_s=Decimal(200), burst=40),
        ),
        clock,
    )

    async def ticker() -> None:
        # Deterministic oscillation: 1.10000 / 1.10002 bid, 1 pip spread.
        for i in range(1_000_000):
            bid = Decimal("1.10000") + Decimal("0.00002") * (i % 2)
            source.push_quote("EURUSD", bid, bid + Decimal("0.00010"))
            await asyncio.sleep(0.005)

    ticks = asyncio.create_task(ticker())
    await broker.connect()
    recorder = EventRecorder(broker)
    await broker.subscribe_ticks(["EURUSD"])
    await recorder.wait_for(TickEvent)
    await broker.snapshot()  # reconcile before trading, as the OMS will

    async def teardown() -> None:
        ticks.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ticks
        await broker.close()
        source.close()

    return ContractHarness(
        broker=broker,
        symbol="EURUSD",
        recorder=recorder,
        force_disconnect=broker.simulate_disconnect,
        reconnect=broker.simulate_reconnect,
        teardown=teardown,
    )


# ---------------------------------------------------------------------------------------------- ctrader


async def _ctrader_harness(
    broker: CTraderBroker, symbol: str, teardown_extra: Callable[[], Awaitable[None]]
) -> ContractHarness:
    await broker.connect()
    recorder = EventRecorder(broker)
    await broker.subscribe_ticks([symbol])
    await recorder.wait_for(TickEvent, timeout=15)
    await broker.snapshot()

    dropped_at = [0]

    async def drop() -> None:
        dropped_at[0] = recorder.mark
        await broker.drop_connection()

    async def wait_up() -> None:
        """The adapter reconnects by itself: wait for DOWN, then UP, after the forced drop."""
        after = dropped_at[0]
        await recorder.wait_for(
            ConnectionEvent, lambda e: e.state is ConnectionState.DOWN, after=after, timeout=30
        )
        downs = [i for i, e in enumerate(recorder.events) if isinstance(e, ConnectionEvent) and i >= after]
        await recorder.wait_for(
            ConnectionEvent, lambda e: e.state is ConnectionState.UP, after=downs[0], timeout=30
        )

    async def teardown() -> None:
        await broker.close()
        await teardown_extra()

    return ContractHarness(
        broker=broker,
        symbol=symbol,
        recorder=recorder,
        force_disconnect=drop,
        reconnect=wait_up,
        teardown=teardown,
    )


async def ctrader_fake_harness() -> ContractHarness:
    from tests.integration.ctrader_fake import FakeCTraderServer

    server = FakeCTraderServer()
    port = await server.start()
    settings = CTraderSettings(
        account_id="contract-ctrader-fake",
        ctid_trader_account_id=server.ctid,
        environment="demo",
        client_id=server.client_id,
        client_secret=SecretStr(server.client_secret),
        symbols=("EURUSD",),
        host="127.0.0.1",
        port=port,
        use_tls=False,
        reconnect_initial_s=0.05,
        account_poll_s=0.5,
    )
    tokens = TokenSet(SecretStr(server.access_token), SecretStr(server.refresh_token), expires_at=None)
    broker = CTraderBroker(
        settings, tokens=tokens, store=TokenStore(Path("/nonexistent"), None), clock=SystemClock()
    )
    return await _ctrader_harness(broker, "EURUSD", server.stop)


DEMO_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"
CTRADER_DEMO_REQUIRED = (
    "CTRADER_DEMO_CLIENT_ID",
    "CTRADER_DEMO_CLIENT_SECRET",
    "CTRADER_DEMO_ACCESS_TOKEN",
    "CTRADER_DEMO_REFRESH_TOKEN",
    "CTRADER_DEMO_ACCOUNT_ID",
)


def demo_env() -> dict[str, str]:
    return {**read_env_file(DEMO_ENV_FILE), **os.environ}


async def ctrader_demo_harness() -> ContractHarness:
    """DEMO ONLY. Uses CTRADER_DEMO_* credentials from the environment or .env."""
    env = demo_env()
    missing = [k for k in CTRADER_DEMO_REQUIRED if not env.get(k)]
    if missing:
        pytest.skip(f"cTrader demo credentials missing: {', '.join(missing)}")
    from trader.brokers.registry import BrokerDeps, default_registry
    from trader.config.settings import AccountConfig

    symbol = env.get("CTRADER_DEMO_SYMBOL", "EURUSD")
    account = AccountConfig.model_validate(
        {
            "id": "contract-ctrader-demo",
            "broker": "ctrader",
            "environment": "demo",  # never live
            "account_id": env["CTRADER_DEMO_ACCOUNT_ID"],
            "secrets_ref": "CTRADER_DEMO",
            "symbols": [symbol],
            "ctrader": {"proxy": env.get("CTRADER_PROXY") or None},
        }
    )
    broker = default_registry().create(account, BrokerDeps(clock=SystemClock(), env=env))
    assert isinstance(broker, CTraderBroker)

    async def nothing() -> None:
        return None

    return await _ctrader_harness(broker, symbol, nothing)


# ---------------------------------------------------------------------------------------------- exness


async def _exness_harness(
    broker: ExnessBroker, symbol: str, teardown_extra: Callable[[], Awaitable[None]]
) -> ContractHarness:
    await broker.connect()
    recorder = EventRecorder(broker)
    await broker.subscribe_ticks([symbol])
    await recorder.wait_for(TickEvent, timeout=15)
    await broker.snapshot()
    dropped_at = [0]

    async def drop() -> None:
        dropped_at[0] = recorder.mark
        await broker.drop_connection()

    async def wait_up() -> None:
        after = dropped_at[0]
        await recorder.wait_for(
            ConnectionEvent, lambda e: e.state is ConnectionState.DOWN, after=after, timeout=30
        )
        downs = [i for i, e in enumerate(recorder.events) if isinstance(e, ConnectionEvent) and i >= after]
        await recorder.wait_for(
            ConnectionEvent, lambda e: e.state is ConnectionState.UP, after=downs[0], timeout=30
        )

    async def teardown() -> None:
        await broker.close()
        await teardown_extra()

    return ContractHarness(
        broker=broker,
        symbol=symbol,
        recorder=recorder,
        force_disconnect=drop,
        reconnect=wait_up,
        teardown=teardown,
    )


async def exness_fake_harness() -> ContractHarness:
    from tests.integration.exness_fake import ACCOUNT, API_KEY, FakeExness

    fake = FakeExness()
    await fake.start()
    settings = ExnessSettings(
        account_id="contract-exness-fake",
        exness_account_id=ACCOUNT,
        gateway_url="http://gateway.test",
        api_key=API_KEY,
        private_key=SecretStr(fake.private_key_hex),
        symbols=("EURUSD",),
        symbol_suffix="m",
        allow_insecure=True,
        trust_env=False,
        reconnect_initial_s=0.05,
        ws_subprotocol=None,
    )
    broker = ExnessBroker(settings, clock=SystemClock(), http_transport=fake.transport)
    return await _exness_harness(broker, "EURUSD", fake.stop)


EXNESS_DEMO_REQUIRED = (
    "EXNESS_DEMO_API_KEY",
    "EXNESS_DEMO_ACCOUNT_ID",
    "EXNESS_DEMO_GATEWAY_URL",
    "EXNESS_DEMO_CONFIRM_DEMO_ACCOUNT",
)


async def exness_demo_harness() -> ContractHarness:
    """DEMO ONLY. Exness does not separate demo and real accounts by host, so the tests refuse to run
    unless EXNESS_DEMO_CONFIRM_DEMO_ACCOUNT=yes declares that EXNESS_DEMO_ACCOUNT_ID is a demo account."""
    env = demo_env()
    missing = [k for k in EXNESS_DEMO_REQUIRED if not env.get(k)]
    if not env.get("EXNESS_DEMO_PRIVATE_KEY") and not env.get("EXNESS_DEMO_PRIVATE_KEY_FILE"):
        missing.append("EXNESS_DEMO_PRIVATE_KEY or EXNESS_DEMO_PRIVATE_KEY_FILE")
    if missing:
        pytest.skip(f"Exness demo credentials missing: {', '.join(missing)}")
    if env["EXNESS_DEMO_CONFIRM_DEMO_ACCOUNT"].strip().lower() != "yes":
        pytest.skip("set EXNESS_DEMO_CONFIRM_DEMO_ACCOUNT=yes only if the account is a DEMO account")
    from trader.brokers.registry import BrokerDeps, default_registry
    from trader.config.settings import AccountConfig

    symbol = env.get("EXNESS_DEMO_SYMBOL", "EURUSD")
    account = AccountConfig.model_validate(
        {
            "id": "contract-exness-demo",
            "broker": "exness",
            "environment": "demo",
            "account_id": env["EXNESS_DEMO_ACCOUNT_ID"],
            "secrets_ref": "EXNESS_DEMO",
            "symbols": [symbol],
            "exness": {
                "gateway_url": env["EXNESS_DEMO_GATEWAY_URL"],
                "symbol_suffix": env.get("EXNESS_DEMO_SYMBOL_SUFFIX", ""),
            },
        }
    )
    broker = default_registry().create(account, BrokerDeps(clock=SystemClock(), env=env))
    assert isinstance(broker, ExnessBroker)

    async def nothing() -> None:
        return None

    return await _exness_harness(broker, symbol, nothing)


HARNESSES: dict[str, Callable[[], Awaitable[ContractHarness]]] = {
    "exness-fake": exness_fake_harness,
    "exness-demo": exness_demo_harness,
    "paper": paper_harness,
    "ctrader-fake": ctrader_fake_harness,
    "ctrader-demo": ctrader_demo_harness,
}
