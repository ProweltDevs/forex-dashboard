"""Composition root: wires an account's broker adapter, RiskEngine, OrderManager and AccountRuntime.

Phase 6 provides ``build_account`` and the standalone kill switch behind ``trader kill``. The long-running
``trader run`` (strategies, market data for paper accounts) arrives with the strategy runner in phase 7 and
builds accounts with the same function. Every account always gets a ``RiskEngine``; ``ApproveAll`` is for
tests only.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from trader.brokers.base import Broker, MarketDataSource
from trader.brokers.registry import BrokerDeps, BrokerRegistry, default_registry
from trader.config.risk import RiskConfig
from trader.config.settings import AccountConfig, AppConfig
from trader.domain.enums import KillLevel
from trader.domain.errors import ConfigError
from trader.domain.time import Clock
from trader.observability.logging import get_logger
from trader.oms import AccountRuntime, JsonFileStore, OmsSettings, OrderManager
from trader.risk.engine import RiskEngine

_log = get_logger("trader.app")


@dataclass(frozen=True, slots=True)
class AccountServices:
    account: AccountConfig
    broker: Broker
    risk: RiskEngine
    oms: OrderManager
    runtime: AccountRuntime


def oms_store_path(data_dir: Path, account_id: str) -> Path:
    """One JSON snapshot per account: orders, attribution, emulations and the risk state (kill switch)."""
    return data_dir / "oms" / f"{account_id}.json"


def build_account(
    config: AppConfig,
    account: AccountConfig,
    *,
    env: Mapping[str, str],
    clock: Clock,
    registry: BrokerRegistry | None = None,
    market_data: MarketDataSource | None = None,
    oms_settings: OmsSettings | None = None,
) -> AccountServices:
    deps = BrokerDeps(clock=clock, env=env, data_dir=config.data_dir, market_data=market_data)
    broker = (registry or default_registry()).create(account, deps)
    risk = RiskEngine(account.risk, clock, account_id=account.id)
    store = JsonFileStore(oms_store_path(config.data_dir, account.id))
    oms = OrderManager(broker, risk, store, clock, oms_settings or OmsSettings(account_id=account.id))
    runtime = AccountRuntime(account.id, broker, oms, symbols=account.symbols)
    return AccountServices(account=account, broker=broker, risk=risk, oms=oms, runtime=runtime)


@dataclass(frozen=True, slots=True)
class KillResult:
    level: KillLevel
    positions_left: int
    orders_left: int
    done: bool  # the level's actions completed within the timeout


async def kill_account(
    services: AccountServices,
    level: KillLevel | None,
    reason: str,
    *,
    timeout: timedelta = timedelta(seconds=60),
    out: Callable[[str], object] = print,
) -> KillResult:
    """Standalone kill switch: connect, reconcile, set ``level`` (``None`` = reset), wait until pending
    orders are cancelled (CANCEL_PENDING) and positions closed (FLATTEN_ALL), persist, disconnect.

    The level is stored in the account's OMS snapshot, so the trading process restores it on start. Do not
    run this while another process manages the same account: both would write the snapshot.
    """
    if services.account.broker == "paper":
        raise ConfigError("paper accounts exist only inside the trading process; there is nothing to kill")
    runtime, oms = services.runtime, services.oms
    await runtime.start(warm_up=False)
    try:
        before = oms.view()
        name = services.account.id
        out(f"{name}: {len(before.positions)} positions, {len(before.open_orders)} pending orders")
        if level is None:
            new = await oms.reset_kill_switch(reason)
        else:
            new = await oms.kill_switch(level, reason)
        out(f"{services.account.id}: kill switch {new}")
        done = await _wait_done(oms, new, timeout)
        await oms.flush()
        view = oms.view()
        return KillResult(
            level=new, positions_left=len(view.positions), orders_left=len(view.open_orders), done=done
        )
    finally:
        await runtime.stop()


async def _wait_done(oms: OrderManager, level: KillLevel, timeout: timedelta) -> bool:
    def done() -> bool:
        view = oms.view()
        if level.rank >= KillLevel.FLATTEN_ALL.rank and view.positions:
            return False
        return not (level.rank >= KillLevel.CANCEL_PENDING.rank and view.open_orders)

    try:
        async with asyncio.timeout(timeout.total_seconds()):
            while not done():
                await asyncio.sleep(0.5)
    except TimeoutError:
        return False
    return True


def risk_summary(risk: RiskConfig) -> str:
    """One line for the live startup banner."""
    lim, b = risk.limits, risk.breakers

    def show(value: object, unit: str = "") -> str:
        return "off" if value is None else f"{value}{unit}"

    return (
        f"max order {show(lim.max_order_volume, ' lots')}, loss/trade {show(lim.max_loss_per_trade_pct, '%')}"
        f" (SL {'required' if lim.require_sl else 'optional'}), daily loss {show(lim.daily_loss_pct, '%')}"
        f" -> {b.daily_loss_level}, drawdown {show(lim.max_drawdown_pct, '%')} -> {b.drawdown_level},"
        f" margin {show(lim.max_margin_usage_pct, '%')}, {len(risk.strategies)} strategy overrides"
    )
