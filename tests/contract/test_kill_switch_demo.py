"""DEMO-ONLY phase 6 acceptance: the kill switch flattens a real demo account.

Run with:  uv run pytest -m demo tests/contract/test_kill_switch_demo.py -s

Uses the production path (trader.app.build_account: broker + RiskEngine + OMS). It opens one minimum-size
position and one far-away pending order through the risk engine, then FLATTEN_ALL must close and cancel
them. FLATTEN_ALL acts on the whole account, so the test refuses to run unless the demo account starts flat.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from trader.app import build_account
from trader.config.settings import AccountConfig, AppConfig
from trader.domain import KillLevel, OrderIntent, OrderStatus, OrderType, RiskStateEvent, Side
from trader.domain.time import SystemClock

from .harness import ctrader_demo_account, demo_env, exness_demo_account

pytestmark = pytest.mark.demo


async def _eventually(check: Callable[[], Awaitable[bool]], timeout: float, what: str) -> None:
    async with asyncio.timeout(timeout):
        while not await check():
            await asyncio.sleep(0.5)
    print(f"[demo] {what}")


@pytest.mark.parametrize("broker", ["ctrader", "exness"])
async def test_kill_switch_flatten_on_demo(broker: str, tmp_path: Path) -> None:
    env = demo_env()
    account: AccountConfig = ctrader_demo_account(env) if broker == "ctrader" else exness_demo_account(env)
    assert account.environment == "demo"
    await flatten_scenario(tmp_path, account, env)


async def flatten_scenario(data_dir: Path, account: AccountConfig, env: Mapping[str, str]) -> None:
    """Shared with tests/integration/test_kill_switch.py, which runs it against the cTrader fake."""
    config = AppConfig(mode="demo", data_dir=data_dir, accounts=(account,))  # type: ignore[arg-type]
    services = build_account(config, account, env=env, clock=SystemClock())
    oms, symbol = services.oms, account.symbols[0]
    sub = oms.events.subscribe("demo-test")
    await services.runtime.start(warm_up=False)
    try:
        view = oms.view()
        if view.positions or view.open_orders:
            pytest.skip(f"{account.id} must start flat (FLATTEN_ALL closes everything on the account)")
        for _ in range(30):
            if symbol in oms.view().last_ticks:
                break
            await asyncio.sleep(0.5)
        else:
            pytest.skip(f"no {symbol} ticks (market closed?)")
        inst = oms.view().instruments[symbol]
        bid = oms.view().last_ticks[symbol].bid

        def intent(**kw: object) -> OrderIntent:
            fields: dict[str, object] = {
                "symbol": symbol,
                "side": Side.BUY,
                "order_type": OrderType.MARKET,
                "volume": inst.min_volume,
                "strategy_id": "demo-kill-test",
                "strategy_version": "1",
                "signal_ts": datetime.now(UTC),
                "sl": inst.round_price(bid * Decimal("0.99")),
            }
            fields.update(kw)
            return OrderIntent(**fields)  # type: ignore[arg-type]

        entry = await oms.submit(intent())
        if entry.status is OrderStatus.REJECTED and "risk:session" in (entry.rejection_reason or ""):
            pytest.skip(f"outside trading hours: {entry.rejection_reason}")
        assert entry.status is not OrderStatus.REJECTED, entry.rejection_reason
        limit = inst.round_price(bid * Decimal("0.99"))
        pending = await oms.submit(
            intent(
                order_type=OrderType.LIMIT,
                limit_price=limit,
                sl=inst.round_price(limit * Decimal("0.99")),
                signal_ts=datetime.now(UTC),
                strategy_id="demo-kill-test-limit",  # same strategy+side+symbol = duplicate signal
            )
        )
        assert pending.status is not OrderStatus.REJECTED, pending.rejection_reason

        async def exposed() -> bool:
            return bool(await services.broker.positions()) and bool(await services.broker.open_orders())

        await _eventually(exposed, 30, "position open and limit order working")

        level = await oms.kill_switch(KillLevel.FLATTEN_ALL, "phase 6 demo acceptance")
        assert level is KillLevel.FLATTEN_ALL

        async def flat() -> bool:
            return not await services.broker.positions() and not await services.broker.open_orders()

        await _eventually(flat, 30, "FLATTEN_ALL: broker reports no positions and no pending orders")
        blocked = await oms.submit(intent(signal_ts=datetime.now(UTC), strategy_version="3"))
        assert blocked.rejection_reason is not None
        assert blocked.rejection_reason.startswith("risk:kill_switch")
        events = []
        while (event := sub.get_nowait()) is not None:
            events.append(event)
        assert any(isinstance(e, RiskStateEvent) and e.level is KillLevel.FLATTEN_ALL for e in events)
        await oms.reset_kill_switch("demo test done")
    finally:
        with contextlib.suppress(Exception):  # never leave demo exposure behind
            for order in await services.broker.open_orders():
                if order.broker_order_id:
                    await services.broker.cancel_order(order.broker_order_id)
            for pos in await services.broker.positions():
                if pos.strategy_id.startswith("demo-kill-test"):
                    await services.broker.close_position(pos.position_id)
        await services.runtime.stop()
