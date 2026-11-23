"""`trader run` end to end (trader.app.App): a paper account on the synthetic feed running the three example
strategies on second bars for a few seconds of real time, then a graceful shutdown and a restart that
restores strategy state; and a paper account fed by a real adapter (the cTrader fake) via BrokerFeed."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from tests.integration.ctrader_fake import FakeCTraderServer
from trader.app import App, run_app
from trader.config.settings import AppConfig
from trader.domain import OrderStatus, StrategyEvent
from trader.domain.time import SystemClock
from trader.strategy.base import StrategyStatus

FAST_RISK = {
    "limits": {
        "sessions": {"weekend": None, "rollover": None},  # run on any day
        "duplicate_window_s": None,
        "max_orders_per_minute": 1000,
        "max_orders_per_minute_strategy": 1000,
        "max_order_to_fill_ratio": None,
        "max_positions_account": 50,
        "max_positions_symbol": 20,
        "max_positions_strategy": 20,
        "max_symbol_volume": "100",
        "max_gross_leverage": "100",
        "max_net_leverage": "100",
        "max_margin_usage_pct": "90",
    },
}


def strategies(account: str, symbol: str) -> list[dict[str, Any]]:
    common = {"account": account, "symbols": [symbol], "timeframes": ["S1"]}
    return [
        {"id": "ema", "class": "ema_cross", "params": {"fast": 2, "slow": 4, "atr_period": 2}, **common},
        {
            "id": "donchian",
            "class": "donchian_breakout",
            "params": {"period": 3, "atr_period": 2, "exit_after_bars": 3},
            **common,
        },
        {
            "id": "rsi",
            "class": "rsi_reversion",
            "params": {"rsi_period": 3, "atr_period": 2, "oversold": "40", "overbought": "60"},
            **common,
        },
    ]


def synthetic_config(data_dir: Path) -> AppConfig:
    return AppConfig.model_validate(
        {
            "mode": "paper",
            "data_dir": str(data_dir),
            "market_data": {"timeframes": ["S1", "S5"], "warmup_bars": 0, "bar_grace_ms": 0},
            "accounts": [
                {
                    "id": "paper-main",
                    "broker": "paper",
                    "symbols": ["EURUSD"],
                    "paper": {"synthetic_feed": {"seed": 7, "tick_interval_ms": 20, "volatility": 0.0004}},
                    "risk": FAST_RISK,
                }
            ],
            "strategies": strategies("paper-main", "EURUSD"),
        }
    )


async def test_trader_run_paper_synthetic_end_to_end_and_restart(tmp_path: Path) -> None:
    config = synthetic_config(tmp_path)
    app = App(config, env={}, clock=SystemClock())
    events: list[StrategyEvent] = []
    stop = asyncio.Event()
    run = asyncio.create_task(run_app(app, stop, duration=timedelta(seconds=8)))
    while app.runner is None:
        await asyncio.sleep(0.05)
    sub = app.runner.events.subscribe("test")

    async def collect() -> None:
        async for e in sub:
            events.append(e)

    collector = asyncio.create_task(collect())
    report = await run
    collector.cancel()

    by_id = {s["id"]: s for s in report.strategies}
    assert set(by_id) == {"ema", "donchian", "rsi"}
    assert report.strategy_errors == 0, [s["last_error"] for s in report.strategies]
    assert all(int(str(s["callbacks"])) > 3 for s in report.strategies)  # on_start + several bars each
    assert sum(int(str(s["intents"])) for s in report.strategies) >= 1, report.lines()
    [account] = report.accounts
    orders = account["orders"]
    assert isinstance(orders, dict)
    assert int(str(orders.get(str(OrderStatus.FILLED), 0))) >= 1, report.lines()
    assert not [e for e in events if e.kind in ("error", "slow_callback")]
    # Persisted on the way out: strategy state, OMS snapshot (incl. risk), a run report. Synthetic
    # random-walk ticks are never recorded: they would pollute the dataset the backtester replays.
    assert json.loads((tmp_path / "strategies" / "ema.json").read_text()).get("above:EURUSD") is not None
    assert (tmp_path / "oms" / "paper-main.json").exists()
    [saved] = list((tmp_path / "reports").glob("run-*.json"))
    assert json.loads(saved.read_text())["strategy_errors"] == 0
    assert not list((tmp_path / "ticks").rglob("*.parquet"))

    # Restart: state is restored before on_start.
    again = App(config, env={}, clock=SystemClock())
    await again.start()
    try:
        assert again.runner is not None
        handle = again.runner.handles["ema"]
        assert handle.status is StrategyStatus.RUNNING
        assert "above:EURUSD" in handle.ctx.state
    finally:
        await again.stop()


@pytest.fixture
async def ctrader() -> AsyncIterator[FakeCTraderServer]:
    s = FakeCTraderServer()
    await s.start()
    yield s
    await s.stop()


async def test_paper_account_fed_by_a_real_adapter(ctrader: FakeCTraderServer, tmp_path: Path) -> None:
    config = AppConfig.model_validate(
        {
            "mode": "paper",
            "data_dir": str(tmp_path),
            "market_data": {"timeframes": ["S1"], "warmup_bars": 0},
            "accounts": [
                {
                    "id": "ct-data",
                    "broker": "ctrader",
                    "environment": "demo",
                    "account_id": str(ctrader.ctid),
                    "secrets_ref": "CT_DEMO",
                    "symbols": ["EURUSD"],
                    "ctrader": {"host": "127.0.0.1", "port": ctrader.port, "use_tls": False},
                },
                {
                    "id": "paper-ct",
                    "broker": "paper",
                    "symbols": ["EURUSD"],
                    "paper": {"market_data_from": "ct-data"},
                    "risk": FAST_RISK,
                },
            ],
        }
    )
    env = {
        "CT_DEMO_CLIENT_ID": ctrader.client_id,
        "CT_DEMO_CLIENT_SECRET": ctrader.client_secret,
        "CT_DEMO_ACCESS_TOKEN": ctrader.access_token,
        "CT_DEMO_REFRESH_TOKEN": ctrader.refresh_token,
    }
    app = App(config, env=env, clock=SystemClock())
    await app.start()
    try:
        assert set(app.accounts) == {"paper-ct"}  # the cTrader account is a data source only
        assert set(app.feeds) == {"ct-data"}
        for _ in range(100):
            if app.market.last_tick("EURUSD", "paper-ct") is not None:
                break
            await asyncio.sleep(0.05)
        assert app.market.last_tick("EURUSD", "paper-ct") is not None  # cTrader quotes reach the paper feed
        assert ctrader.orders == {}
        assert ctrader.positions == {}  # nothing was ever traded on the data account
    finally:
        await app.stop()


def test_config_rejects_strategies_on_data_sources_and_bad_symbols(tmp_path: Path) -> None:
    base = synthetic_config(tmp_path).model_dump(by_alias=True, mode="json")
    bad_symbol = json.loads(json.dumps(base))
    bad_symbol["strategies"][0]["symbols"] = ["GBPUSD"]
    with pytest.raises(ValueError, match="not in account"):
        AppConfig.model_validate(bad_symbol)
    bad_tf = json.loads(json.dumps(base))
    bad_tf["strategies"][0]["timeframes"] = ["H4"]
    with pytest.raises(ValueError, match=r"market_data\.timeframes"):
        AppConfig.model_validate(bad_tf)
    dup = json.loads(json.dumps(base))
    dup["strategies"][1]["id"] = "ema"
    with pytest.raises(ValueError, match="unique"):
        AppConfig.model_validate(dup)


async def test_portfolio_matches_the_broker_statement_and_writes_the_books(tmp_path: Path) -> None:
    """Phase 8 on paper: the ledger built from live fills equals the broker's deal history (pnl-check),
    and the books (fills, equity, journal, executions, daily report) are written."""
    from datetime import UTC, datetime

    from trader.analytics.statement import compare, day_window

    config = synthetic_config(tmp_path)
    app = App(config, env={}, clock=SystemClock())
    await app.start()
    try:
        books = app.books["paper-main"]
        for _ in range(400):  # until a round trip closed
            if books.journal.closed:
                break
            await asyncio.sleep(0.05)
        assert books.journal.closed
        await asyncio.sleep(0.3)  # let in-flight fills land
        day = books.portfolio.ledger_day(datetime.now(UTC))
        start, end = day_window(day, 0)
        broker = await app.accounts["paper-main"].broker.deal_history(start, end)
        ours = [e.fill for e in books.portfolio.ledger.entries(day=day)]
        check = compare(day, ours, broker)
        assert check.ok, check.lines()
        assert check.chain_checked > 0  # paper reports the balance after every deal
        point = books.portfolio.sample(datetime.now(UTC))
        assert point is not None
        acct = app.accounts["paper-main"].oms.view().account
        assert acct is not None
        assert abs(point.balance - acct.balance) <= Decimal("0.01") * len(ours)  # computed = broker balance
        trade = books.journal.closed[0]
        assert trade.strategy_id in {"ema", "donchian", "rsi"}
        assert trade.params_hash
        assert trade.initial_sl is not None
        assert trade.risk is not None
        assert books.executions.records
        rec = books.executions.records[0]
        assert rec.latency_ms("total") is not None
        assert rec.slippage_points is not None
        deals = len(books.portfolio.ledger)
    finally:
        await app.stop()
    portfolio = tmp_path / "portfolio" / "paper-main"
    assert (portfolio / "fills.jsonl").read_text().count("\n") >= deals > 0
    assert list(portfolio.glob("equity-*.jsonl"))
    assert (tmp_path / "journal" / "paper-main" / "trades.jsonl").exists()
    assert (tmp_path / "executions" / "paper-main.jsonl").exists()
    report = json.loads((tmp_path / "reports" / "paper-main" / f"{day.isoformat()}.json").read_text())
    assert report["trades"]["trades"] >= 1
    assert report["execution"]["overall"]["orders"] >= 1
    assert "approved" in report["risk"]


async def test_pnl_check_against_a_real_adapter_and_report_cli(
    ctrader: FakeCTraderServer, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """cTrader (fake): trade a round trip through the OMS, stop, then `pnl-check` the day against the broker's
    deal statement (incl. the balance chain from closing deals) and build the report from stored books."""
    from datetime import UTC, datetime

    from trader.app import run_pnl_check
    from trader.brokers.ctrader import TokenStore
    from trader.cli import main
    from trader.domain import OrderIntent, OrderType, Side

    config = AppConfig.model_validate(
        {
            "mode": "demo",
            "data_dir": str(tmp_path),
            "market_data": {"timeframes": ["S1"], "warmup_bars": 0},
            "accounts": [
                {
                    "id": "ct",
                    "broker": "ctrader",
                    "environment": "demo",
                    "account_id": str(ctrader.ctid),
                    "secrets_ref": "CT_DEMO",
                    "symbols": ["EURUSD"],
                    "ctrader": {"host": "127.0.0.1", "port": ctrader.port, "use_tls": False},
                    "risk": FAST_RISK,
                },
            ],
        }
    )
    # cTrader rotates OAuth tokens: the key lets the token store hand them to the next process (pnl-check).
    env = {
        "CT_DEMO_CLIENT_ID": ctrader.client_id,
        "CT_DEMO_CLIENT_SECRET": ctrader.client_secret,
        "CT_DEMO_ACCESS_TOKEN": ctrader.access_token,
        "CT_DEMO_REFRESH_TOKEN": ctrader.refresh_token,
        "TRADER_SECRETS_KEY": TokenStore.generate_key(),
    }
    app = App(config, env=env, clock=SystemClock())
    await app.start()
    try:
        oms, books = app.accounts["ct"].oms, app.books["ct"]
        for _ in range(100):
            if "EURUSD" in oms.view().last_ticks:
                break
            await asyncio.sleep(0.05)
        bid = oms.view().last_ticks["EURUSD"].bid
        order = await oms.submit(
            OrderIntent(
                symbol="EURUSD",
                side=Side.BUY,
                order_type=OrderType.MARKET,
                volume=Decimal("0.10"),
                strategy_id="manual-test",
                strategy_version="1",
                signal_ts=datetime.now(UTC),
                sl=bid - Decimal("0.00300"),
            )
        )
        assert order.rejection_reason is None, order.rejection_reason
        for _ in range(100):
            if oms.positions():
                break
            await asyncio.sleep(0.05)
        await oms.close_position(oms.positions()[0].position_id)
        for _ in range(100):
            if books.journal.closed:
                break
            await asyncio.sleep(0.05)
        [trade] = books.journal.closed
        assert (trade.strategy_id, trade.commission) == ("manual-test", Decimal("-0.60"))
    finally:
        await app.stop()

    day = trade.exit_time.date()
    check = await run_pnl_check(config, config.accounts[0], day, env=env, clock=SystemClock())
    assert check.ok, check.lines()
    assert (check.broker.deals, check.chain_checked) == (2, 0)  # one closing deal: the chain needs two
    assert check.ours.net == trade.net

    fills = tmp_path / "portfolio" / "ct" / "fills.jsonl"
    lines = fills.read_text().splitlines()
    fills.write_text(lines[0] + "\n")  # lose the closing deal
    broken = await run_pnl_check(config, config.accounts[0], day, env=env, clock=SystemClock())
    assert not broken.ok
    assert [f.realized_pnl is not None for f in broken.missing] == [True]

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(json.dumps(config.model_dump(mode="json", by_alias=True)))
    assert (
        main(
            [
                "--env-file",
                str(tmp_path / "none.env"),
                "report",
                "--config",
                str(cfg_file),
                "--account",
                "ct",
                "--date",
                day.isoformat(),
            ],
            env={},
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "1 trades" in out
    assert (tmp_path / "reports" / "ct" / f"{day.isoformat()}-trades.csv").exists()
