"""`trader kill` path (trader.app.kill_account) against the in-process cTrader fake: built from an
AccountConfig + env exactly like the CLI, it reconciles, flattens, persists the level, and resets."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tests.integration.ctrader_fake import FakeCTraderServer
from tests.integration.test_ctrader_adapter import make_broker, market
from trader.app import build_account, kill_account, oms_store_path
from trader.cli import main
from trader.config.settings import AppConfig
from trader.domain import KillLevel, OrderType
from trader.domain.time import SystemClock
from trader.oms import JsonFileStore

D = Decimal
# Never the developer's real ./.env: tests must not see real credentials.
NO_DOTENV = ["--env-file", "/nonexistent/trader-tests.env"]
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
async def server() -> AsyncIterator[FakeCTraderServer]:
    s = FakeCTraderServer()
    await s.start()
    yield s
    await s.stop()


def config(server: FakeCTraderServer, data_dir: Path) -> AppConfig:
    return AppConfig.model_validate(
        {
            "mode": "demo",
            "data_dir": str(data_dir),
            "accounts": [
                {
                    "id": "ct-demo",
                    "broker": "ctrader",
                    "environment": "demo",
                    "account_id": str(server.ctid),
                    "secrets_ref": "CT_DEMO",
                    "symbols": ["EURUSD"],
                    "ctrader": {"host": "127.0.0.1", "port": server.port, "use_tls": False},
                }
            ],
        }
    )


def env(server: FakeCTraderServer) -> dict[str, str]:
    return {
        "CT_DEMO_CLIENT_ID": server.client_id,
        "CT_DEMO_CLIENT_SECRET": server.client_secret,
        "CT_DEMO_ACCESS_TOKEN": server.access_token,
        "CT_DEMO_REFRESH_TOKEN": server.refresh_token,
    }


async def open_exposure(server: FakeCTraderServer) -> None:
    """Another session (e.g. the trading process before it died) leaves a position and a pending order."""
    broker = make_broker(server)
    await broker.connect()
    try:
        await broker.subscribe_ticks(["EURUSD"])
        await asyncio.sleep(0.2)  # first quote
        await broker.place_order(market(volume="0.10"))
        await broker.place_order(market(order_type=OrderType.LIMIT, limit_price=D("1.05000")))
        for _ in range(100):
            if server.positions and server.orders:
                break
            await asyncio.sleep(0.02)
    finally:
        await broker.close()
    assert len(server.positions) == 1
    assert len(server.orders) == 1


async def test_kill_flattens_persists_and_resets(server: FakeCTraderServer, tmp_path: Path) -> None:
    await open_exposure(server)
    cfg = config(server, tmp_path)
    lines: list[str] = []

    services = build_account(cfg, cfg.accounts[0], env=env(server), clock=SystemClock())
    result = await kill_account(
        services, KillLevel.FLATTEN_ALL, "test", timeout=timedelta(seconds=10), out=lines.append
    )
    assert (result.level, result.positions_left, result.orders_left, result.done) == (
        KillLevel.FLATTEN_ALL,
        0,
        0,
        True,
    )
    assert server.positions == {}
    assert server.orders == {}
    assert lines[0] == "ct-demo: 1 positions, 1 pending orders"

    state = JsonFileStore(oms_store_path(tmp_path, "ct-demo")).load()
    assert state is not None
    triggers = state.risk["triggers"]
    assert isinstance(triggers, dict)
    assert set(triggers) == {"manual"}

    # A new session (the trading process starting up) restores the switch and refuses entries.
    again = build_account(cfg, cfg.accounts[0], env=env(server), clock=SystemClock())
    assert (
        await kill_account(again, None, "reviewed", timeout=timedelta(seconds=5), out=lines.append)
    ).level is (KillLevel.NONE)
    assert any("kill switch NONE" in line for line in lines)
    state = JsonFileStore(oms_store_path(tmp_path, "ct-demo")).load()
    assert state is not None
    assert state.risk["triggers"] == {}


async def test_demo_flatten_scenario_on_the_fake(server: FakeCTraderServer, tmp_path: Path) -> None:
    """The demo acceptance test's exact flow, run against the fake so its logic is checked on every run."""
    from tests.contract.test_kill_switch_demo import flatten_scenario

    base = config(server, tmp_path).accounts[0]
    # Session rules off so this runs on any day (the demo run keeps the defaults).
    account = base.model_copy(
        update={
            "risk": base.risk.model_validate({"limits": {"sessions": {"weekend": None, "rollover": None}}})
        }
    )
    await flatten_scenario(tmp_path, account, env(server))
    assert server.positions == {}
    assert server.orders == {}


def test_cli_kill_refuses_unknown_and_paper_accounts(capsys: pytest.CaptureFixture[str]) -> None:
    paper = str(ROOT / "config/paper.yaml")
    assert (
        main(
            ["kill", "--config", paper, "--account", "nope", "--level", "PAUSE_NEW", "--reason", "x"], env={}
        )
        == 2
    )
    assert "unknown account" in capsys.readouterr().err
    assert (
        main(
            [*NO_DOTENV, "kill", "--config", paper, "--account", "paper-main", "--reset", "--reason", "x"],
            env={},
        )
        == 2
    )
    assert "kill failed" in capsys.readouterr().err


def test_cli_kill_on_live_needs_confirmation(capsys: pytest.CaptureFixture[str]) -> None:
    live = str(ROOT / "config/live.yaml.example")
    args = ["kill", "--config", live, "--account", "ctrader-live", "--level", "FLATTEN_ALL", "--reason", "x"]
    assert main(args, env={"TRADER_MODE": "live"}) == 2
    assert "confirm-live" in capsys.readouterr().err
