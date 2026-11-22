"""Phase 10 acceptance: alerts fire. The real ``App`` on the paper broker notifies through the real
Telegram channel (HTTP) to a fake Bot API server: startup, rejects, fills and SL hits, the kill switch
(set by a Telegram ``/kill`` command, audited), risk cleared, disconnect/reconnect, operator alerts,
dedupe, and shutdown. Commands from another chat or a group are ignored."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path

from tests.integration.api_helpers import ACCOUNT, Running, api_config, running_app, wait_for
from tests.integration.telegram_fake import BOT_TOKEN, CHAT_ID, FakeTelegram, fake_telegram
from trader.app import daily_lines
from trader.brokers.paper.adapter import PaperBroker
from trader.config.settings import AppConfig
from trader.domain.enums import KillLevel
from trader.domain.events import AlertEvent


def notify_config(tmp_path: Path, fake: FakeTelegram, *, fills: bool = True) -> AppConfig:
    return api_config(
        tmp_path,
        notify={
            "telegram": {"api_base": fake.url, "commands": True},
            "dedupe_s": 300,
            "rate_per_min": 1000,
            "fills": fills,
        },
    )


ENV = {"TELEGRAM_BOT_TOKEN": BOT_TOKEN, "TELEGRAM_CHAT_ID": CHAT_ID}
TIGHT = Decimal("0.0003")  # the synthetic walk moves ~4 pips a tick: this stop is hit almost at once


async def open_with_tight_sl(run: Running, side: str) -> dict[str, object]:
    """A manual order with a stop just beyond the market. The price can move past the stop between
    reading it and the risk check (the order is then rightly rejected), so retry with a fresh price."""
    for _ in range(20):
        bid, ask = run.price()
        sl = bid - TIGHT if side == "BUY" else ask + TIGHT
        body = {"account": ACCOUNT, "symbol": "EURUSD", "side": side, "volume": "0.01", "sl": str(sl)}
        order: dict[str, object] = (await run.client.post("/orders", json=body)).json()
        if order["status"] != "REJECTED":
            return order
        await asyncio.sleep(0.03)
    raise AssertionError(f"no {side} order with a tight stop was accepted")


async def test_alerts_fire_and_telegram_commands_work(tmp_path: Path) -> None:
    async with fake_telegram() as tg:
        config = notify_config(tmp_path, tg)
        async with running_app(config, env=ENV) as run:
            await _scenario(run, tg)
        # Shutdown: the final notice is delivered before the process exits.
        stopped = await tg.wait_text("Trader stopped")
        assert "strategy errors this run: 0" in stopped
    assert all(BOT_TOKEN not in t for t in tg.sent)


async def _scenario(run: Running, tg: FakeTelegram) -> None:
    c = run.client
    started = await tg.wait_text("Trader started")
    assert started.startswith("[paper] INFO: Trader started")
    assert f"{ACCOUNT} (paper demo): kill NONE" in started
    oms = run.app.accounts[ACCOUNT].oms
    await wait_for(lambda: oms.view().last_ticks, what="ticks")

    # Reject: no SL. The same reject again is deduped.
    no_sl = {"account": ACCOUNT, "symbol": "EURUSD", "side": "BUY", "volume": "0.01"}
    await c.post("/orders", json=no_sl)
    await c.post("/orders", json=no_sl)
    reject = await tg.wait_text("Order rejected EURUSD")
    assert "WARNING" in reject
    assert "manual: BUY 0.01 EURUSD MARKET: risk:loss_per_trade" in reject

    # Fills, then an SL is hit: a BUY and a SELL with tight stops on a fast random walk, so whichever way
    # the price goes, one stop is hit within a few ticks.
    await open_with_tight_sl(run, "BUY")
    await open_with_tight_sl(run, "SELL")
    await tg.wait_text("manual: BUY 0.01 EURUSD @")
    await tg.wait_text("manual: SELL 0.01 EURUSD @")
    sl_hit = await tg.wait_text("SL hit EURUSD", timeout=30)
    assert " 0.01 EURUSD closed @" in sl_hit
    assert "P&L -" in sl_hit

    # /kill from the allowed private chat: the kill switch through Control, audited as telegram.
    tg.command("/kill")
    reply = await tg.wait_text(f"{ACCOUNT}: kill switch PAUSE_NEW")
    assert "[paper]" not in reply  # a reply, not a notice
    kill = await tg.wait_text("CRITICAL: Kill switch PAUSE_NEW")
    assert "manual" in kill
    assert oms.risk_level is KillLevel.PAUSE_NEW
    [audit] = (await c.get("/audit?action=kill")).json()
    assert audit["actor"] == "telegram"

    # Ignored: another chat, a group with the right id, an old message.
    tg.command("/kill flatten", chat_id="999")
    tg.command("/kill flatten", chat_type="group")
    tg.command("/kill flatten", age_s=600)
    tg.command("/status")
    status = await tg.wait_text("(paper demo): connected, kill PAUSE_NEW")
    assert "strategies: PAUSED" in status
    assert oms.risk_level is KillLevel.PAUSE_NEW  # never FLATTEN_ALL
    tg.command("/pnl")
    await tg.wait_text(f"{ACCOUNT} ")
    tg.command("/nope")
    await tg.wait_text("unknown command /nope")

    # Reset (API only: the bot cannot reset) -> risk cleared.
    await c.post("/risk/reset", json={"account": ACCOUNT, "reason": "ok"})
    await tg.wait_text("Risk cleared")

    # Disconnect / reconnect.
    broker = run.app.accounts[ACCOUNT].broker
    assert isinstance(broker, PaperBroker)
    await broker.simulate_disconnect("test outage")
    down = await tg.wait_text("WARNING: Disconnected")
    assert "test outage" in down
    await broker.simulate_reconnect()
    await tg.wait_text("INFO: Reconnected")

    # An operator alert (as the portfolio's drift check publishes it).
    books = run.app.books[ACCOUNT]
    books.portfolio.events.publish(
        AlertEvent(
            account_id=ACCOUNT,
            kind="equity_drift",
            severity="warning",
            message="gap 12.00",
            ts=run.app.clock.now(),
        )
    )
    drift = await tg.wait_text("equity drift")
    assert f"{ACCOUNT}: gap 12.00" in drift

    # The daily report's lines (sent at each trading-day rollover).
    lines = daily_lines(books, books.portfolio.ledger_day(run.app.clock.now()))
    assert lines[0].startswith("realized ")
    assert any("manual EURUSD" in line for line in lines)

    notifier = run.app.notifications
    assert notifier is not None
    assert notifier.notifier.stats.deduped >= 1
    assert notifier.notifier.stats.failed == 0
    assert len(tg.texts("risk:loss_per_trade")) == 1  # the two identical rejects: sent once


async def test_without_fill_notices_sl_hits_still_fire(tmp_path: Path) -> None:
    async with fake_telegram() as tg, running_app(notify_config(tmp_path, tg, fills=False), env=ENV) as run:
        await wait_for(lambda: run.app.accounts[ACCOUNT].oms.view().last_ticks, what="ticks")
        await open_with_tight_sl(run, "BUY")
        await open_with_tight_sl(run, "SELL")
        await tg.wait_text("SL hit EURUSD", timeout=30)
        assert not [t for t in tg.sent if "INFO: Fill " in t]
