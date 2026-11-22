"""DEMO-ONLY phase 8 acceptance: P&L matches the broker statement for a demo day.

Run with:  uv run pytest -m demo tests/contract/test_pnl_demo.py -s

The real ``App`` runs on a demo account (broker + risk + OMS + portfolio books). It trades one minimum-size
round trip through risk and the OMS, then:
1. our fill ledger for the trading day (live fills + the startup backfill of today's earlier deals) must
   equal the broker's deal statement for the day, deal by deal and in total (``trader pnl-check``), with
   the balance chain where the broker reports balances;
2. the balance change over the round trip must equal the trade's net (gross + commission + swap).
Tolerance: 0.01 account currency per deal (documented in ADR 0010). The account must start flat.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from trader.analytics.statement import compare, day_window
from trader.app import App
from trader.config.settings import AccountConfig, AppConfig
from trader.domain import OrderIntent, OrderStatus, OrderType, Side
from trader.domain.time import SystemClock

from .harness import ctrader_demo_account, demo_env, exness_demo_account

pytestmark = pytest.mark.demo


async def _until(
    check: Callable[[], bool] | Callable[[], Awaitable[bool]], timeout: float, what: str
) -> None:
    async with asyncio.timeout(timeout):
        while True:
            result = check()
            if await result if asyncio.iscoroutine(result) else result:  # type: ignore[misc]
                break
            await asyncio.sleep(0.5)
    print(f"[demo] {what}")


@pytest.mark.parametrize("broker", ["ctrader", "exness"])
async def test_pnl_matches_the_broker_statement_on_demo(broker: str, tmp_path: Path) -> None:
    env = demo_env()
    account: AccountConfig = ctrader_demo_account(env) if broker == "ctrader" else exness_demo_account(env)
    assert account.environment == "demo"
    config = AppConfig(mode="demo", data_dir=tmp_path, accounts=(account,))  # type: ignore[arg-type]
    app = App(config, env=env, clock=SystemClock())
    await app.start()
    symbol = account.symbols[0]
    try:
        oms, books = app.accounts[account.id].oms, app.books[account.id]
        if oms.positions() or oms.view().open_orders:
            pytest.skip(f"{account.id} must start flat")
        await _until(lambda: symbol in oms.view().last_ticks, 15, "price received")
        view = oms.view()
        inst, bid = view.instruments[symbol], view.last_ticks[symbol].bid
        assert view.account is not None
        before = view.account.balance
        order = await oms.submit(
            OrderIntent(
                symbol=symbol,
                side=Side.BUY,
                order_type=OrderType.MARKET,
                volume=inst.min_volume,
                strategy_id="demo-pnl-test",
                strategy_version="1",
                signal_ts=datetime.now(UTC),
                sl=inst.round_price(bid * Decimal("0.99")),
            )
        )
        if order.status is OrderStatus.REJECTED and "risk:session" in (order.rejection_reason or ""):
            pytest.skip(f"outside trading hours: {order.rejection_reason}")
        assert order.status is not OrderStatus.REJECTED, order.rejection_reason
        await _until(lambda: bool(oms.positions()), 30, "position open")
        await oms.close_position(oms.positions()[0].position_id)
        await _until(lambda: bool(books.journal.closed), 30, "round trip closed")
        [trade] = books.journal.closed
        await _until(
            lambda: oms.view().account is not None and oms.view().account.balance != before,
            15,  # type: ignore[union-attr]
            "balance updated",
        )
        after = oms.view().account.balance  # type: ignore[union-attr]

        day = books.portfolio.ledger_day(trade.exit_time)
        statement = await app.accounts[account.id].broker.deal_history(
            *day_window(day, account.risk.day_offset_minutes)
        )
        check = compare(day, [e.fill for e in books.portfolio.ledger.entries(day=day)], statement)
        print("\n".join(check.lines()))
        print(
            f"[demo] trip net {trade.net} (gross {trade.gross}, costs {trade.commission + trade.swap}) "
            f"balance {before} -> {after} ({after - before})"
        )
        assert check.ok
        assert abs((after - before) - trade.net) <= Decimal("0.01") * 2
    finally:
        await app.stop()
