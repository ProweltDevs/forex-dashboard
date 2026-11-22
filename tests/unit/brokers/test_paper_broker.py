"""Deterministic tests of the paper broker's simulation (fixed clock, hand-pushed ticks, no latency)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal

import pytest

from tests.contract.harness import EventRecorder
from tests.factories import eurusd
from trader.brokers.base import OrderChanges, RateLimit
from trader.brokers.paper import FakeTickSource, PaperBroker, PaperSettings
from trader.domain import (
    AccountEvent,
    FillEvent,
    Instrument,
    OrderEvent,
    OrderStatus,
    OrderType,
    PositionChange,
    PositionEvent,
    Side,
    TickEvent,
    Timeframe,
    TimeInForce,
    TradingSession,
)
from trader.domain.errors import (
    BrokerUnavailable,
    ConversionUnavailable,
    InvalidPrice,
    InvalidVolume,
    MarketClosed,
    OrderNotFound,
    OrderRejected,
    PositionNotFound,
    RateLimited,
    UnknownSymbol,
)
from trader.domain.ids import new_client_order_id
from trader.domain.models import OrderRequest
from trader.domain.time import FixedClock

D = Decimal
MONDAY = datetime(2026, 1, 5, 8, 0, tzinfo=UTC)  # a Monday


def usdjpy() -> Instrument:
    return eurusd(
        symbol="USDJPY", broker_symbol="USDJPY", broker_id="2", digits=3, tick_size=D("0.001"),
        base_currency="USD", quote_currency="JPY", margin_currency="USD",
    )  # fmt: skip


def xauusd() -> Instrument:
    return eurusd(
        symbol="XAUUSD", broker_symbol="XAUUSD", broker_id="3", digits=2, tick_size=D("0.01"),
        contract_size=D(100), base_currency="XAU", quote_currency="USD", margin_currency="USD",
    )  # fmt: skip


def gbpjpy() -> Instrument:
    return eurusd(
        symbol="GBPJPY", broker_symbol="GBPJPY", broker_id="4", digits=3, tick_size=D("0.001"),
        base_currency="GBP", quote_currency="JPY", margin_currency="GBP",
    )  # fmt: skip


@dataclass
class Sim:
    broker: PaperBroker
    source: FakeTickSource
    clock: FixedClock
    rec: EventRecorder

    async def tick(
        self, symbol: str, bid: str, ask: str, *, advance: timedelta = timedelta(seconds=1)
    ) -> None:
        now = self.clock.advance(advance)
        self.source.push_quote(symbol, D(bid), D(ask), ts=now)
        await settle()

    async def order(self, side: Side, volume: str = "1", **kw: object) -> tuple[str, str]:
        cid = new_client_order_id()
        fields: dict[str, object] = {
            "client_order_id": cid, "symbol": "EURUSD", "side": side, "order_type": OrderType.MARKET,
            "volume": D(volume), "strategy_id": "s", "strategy_version": "1",
        }  # fmt: skip
        fields.update(kw)
        ack = await self.broker.place_order(OrderRequest(**fields))  # type: ignore[arg-type]
        await settle()
        assert ack.broker_order_id
        return cid, ack.broker_order_id

    def fills(self, cid: str | None = None) -> list[FillEvent]:
        return [e for e in self.rec.of(FillEvent) if cid is None or e.fill.client_order_id == cid]

    def statuses(self, cid: str) -> list[OrderStatus]:
        return [e.order.status for e in self.rec.of(OrderEvent) if e.order.client_order_id == cid]

    def position_changes(self) -> list[PositionChange]:
        return [e.change for e in self.rec.of(PositionEvent)]


async def settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)


async def make_sim(settings: PaperSettings | None = None, instruments: list[Instrument] | None = None) -> Sim:
    clock = FixedClock(MONDAY)
    insts = instruments or [eurusd()]
    source = FakeTickSource(insts, clock)
    broker = PaperBroker(source, settings or PaperSettings(), clock)
    await broker.connect()
    rec = EventRecorder(broker)
    await broker.subscribe_ticks([i.symbol for i in insts])
    sim = Sim(broker, source, clock, rec)
    await sim.tick("EURUSD", "1.10000", "1.10010")
    await broker.snapshot()
    return sim


@pytest.fixture
async def sim() -> AsyncIterator[Sim]:
    s = await make_sim()
    yield s
    await s.rec.stop()
    await s.broker.close()


# ------------------------------------------------------------------ market orders


async def test_market_buy_fills_at_ask_and_sell_at_bid(sim: Sim) -> None:
    buy, _ = await sim.order(Side.BUY)
    sell, _ = await sim.order(Side.SELL)
    assert sim.fills(buy)[0].fill.price == D("1.10010")
    assert sim.fills(sell)[0].fill.price == D("1.10000")
    assert sim.statuses(buy) == [OrderStatus.ACCEPTED, OrderStatus.FILLED]
    assert len(await sim.broker.positions()) == 2  # hedging: two positions


async def test_realized_pnl_commission_and_balance() -> None:
    sim = await make_sim(PaperSettings(commission_per_lot=D("3.5")))
    cid, _ = await sim.order(Side.BUY, "1")
    pid = sim.fills(cid)[0].fill.position_id
    assert sim.fills(cid)[0].fill.commission == D("-3.5")
    await sim.tick("EURUSD", "1.10110", "1.10120")
    pos = (await sim.broker.positions())[0]
    assert pos.unrealized_pnl == D("100.00000")  # (1.10110 - 1.10010) * 100 000

    await sim.broker.close_position(pid)
    await settle()
    close = sim.fills()[-1].fill
    assert close.realized_pnl == D("100.00000")
    assert close.side is Side.SELL
    account = await sim.broker.account()
    assert account.balance == D(10_000) + D(100) - D(7)
    assert account.equity == account.balance
    assert account.margin == 0


async def test_slippage_is_adverse_and_max_slippage_rejects() -> None:
    sim = await make_sim(PaperSettings(slippage_points=3))
    buy, _ = await sim.order(Side.BUY)
    sell, _ = await sim.order(Side.SELL)
    assert sim.fills(buy)[0].fill.price == D("1.10013")
    assert sim.fills(sell)[0].fill.price == D("1.09997")

    capped, _ = await sim.order(Side.BUY, max_slippage=D("0.00002"))
    assert sim.statuses(capped)[-1] is OrderStatus.REJECTED
    assert sim.fills(capped) == []


async def test_extra_spread_widens_both_sides() -> None:
    sim = await make_sim(PaperSettings(extra_spread_points=4))
    buy, _ = await sim.order(Side.BUY)
    sell, _ = await sim.order(Side.SELL)
    assert sim.fills(buy)[0].fill.price == D("1.10012")
    assert sim.fills(sell)[0].fill.price == D("1.09998")


async def test_insufficient_margin_rejected() -> None:
    sim = await make_sim(PaperSettings(starting_balance=D(1000), leverage=10))
    cid, _ = await sim.order(Side.BUY, "1")  # needs 100 000 EUR / 10 = ~11 000 USD margin
    assert sim.statuses(cid)[-1] is OrderStatus.REJECTED
    assert (await sim.broker.positions()) == []


async def test_margin_and_margin_level(sim: Sim) -> None:
    await sim.order(Side.BUY, "1")
    account = await sim.broker.account()
    assert account.margin == D(100_000) / 100 * D("1.10005")  # EUR margin converted at EURUSD mid
    assert account.margin_level == account.equity / account.margin * 100
    assert account.free_margin == account.equity - account.margin


# ------------------------------------------------------------------ validation


async def test_request_validation(sim: Sim) -> None:
    with pytest.raises(InvalidVolume):
        await sim.order(Side.BUY, "0.015")
    with pytest.raises(UnknownSymbol):
        await sim.order(Side.BUY, symbol="GBPUSD")
    with pytest.raises(InvalidPrice):
        await sim.order(Side.BUY, sl=D("1.10020"))  # SL above a buy's entry
    with pytest.raises(InvalidPrice):
        await sim.order(Side.SELL, tp=D("1.10050"))  # TP above a sell's entry
    with pytest.raises(InvalidPrice):
        await sim.order(Side.BUY, order_type=OrderType.LIMIT, limit_price=D("1.20000"))  # limit above ask
    with pytest.raises(InvalidPrice):
        await sim.order(Side.BUY, order_type=OrderType.STOP, stop_price=D("1.00000"))  # stop below ask
    with pytest.raises(InvalidPrice):
        await sim.order(Side.BUY, order_type=OrderType.LIMIT, limit_price=D("1.000001"))  # off tick grid
    with pytest.raises(OrderRejected):
        await sim.order(
            Side.BUY, order_type=OrderType.LIMIT, limit_price=D("1.0"), time_in_force=TimeInForce.IOC
        )


async def test_no_price_and_closed_session() -> None:
    closed = eurusd(sessions=(TradingSession(weekday=6, start=time(22), end=time(23)),))
    sim = await make_sim(instruments=[closed, usdjpy()])
    with pytest.raises(MarketClosed, match="session"):
        await sim.order(Side.BUY)
    with pytest.raises(MarketClosed, match="no price"):
        await sim.order(Side.BUY, symbol="USDJPY")


async def test_conversion_required_for_cross() -> None:
    sim = await make_sim(instruments=[eurusd(), gbpjpy()])
    await sim.tick("GBPJPY", "190.000", "190.020")
    with pytest.raises(ConversionUnavailable):
        await sim.order(Side.BUY, symbol="GBPJPY")


async def test_not_connected_and_not_found(sim: Sim) -> None:
    with pytest.raises(OrderNotFound):
        await sim.broker.cancel_order("nope")
    with pytest.raises(PositionNotFound):
        await sim.broker.close_position("nope")
    await sim.broker.simulate_disconnect()
    with pytest.raises(BrokerUnavailable):
        await sim.broker.account()


async def test_duplicate_client_order_id_is_idempotent(sim: Sim) -> None:
    cid, oid = await sim.order(Side.BUY)
    again = await sim.broker.place_order(
        OrderRequest(client_order_id=cid, symbol="EURUSD", side=Side.BUY, order_type=OrderType.MARKET,
                     volume=D(1), strategy_id="s", strategy_version="1")
    )  # fmt: skip
    await settle()
    assert again.duplicate
    assert again.broker_order_id == oid
    assert len(sim.fills(cid)) == 1


async def test_rate_limit_raises_with_retry_hint() -> None:
    limit = RateLimit(name="requests", rate_per_s=D(10), burst=3)
    sim = await make_sim(PaperSettings(rate_limit=limit))
    await asyncio.sleep(0.35)  # refill after the setup calls
    for _ in range(3):
        await sim.broker.account()
    with pytest.raises(RateLimited) as info:
        await sim.broker.account()
    assert info.value.retry_after_ms is not None
    assert 0 < info.value.retry_after_ms <= 100
    assert (await sim.broker.limits()).get("requests") == limit


# ------------------------------------------------------------------ pending orders


async def test_buy_limit_triggers_on_ask(sim: Sim) -> None:
    cid, _ = await sim.order(Side.BUY, order_type=OrderType.LIMIT, limit_price=D("1.09900"), tp=D("1.11000"))
    assert sim.statuses(cid) == [OrderStatus.ACCEPTED, OrderStatus.WORKING]
    await sim.tick("EURUSD", "1.09895", "1.09905")  # ask above limit: no fill
    assert sim.fills(cid) == []
    await sim.tick("EURUSD", "1.09880", "1.09890")
    fill = sim.fills(cid)[0].fill
    assert fill.price == D("1.09890")
    assert (await sim.broker.positions())[0].tp == D("1.11000")
    assert (await sim.broker.open_orders()) == []


async def test_sell_stop_triggers_with_slippage() -> None:
    sim = await make_sim(PaperSettings(slippage_points=2))
    cid, _ = await sim.order(Side.SELL, order_type=OrderType.STOP, stop_price=D("1.09950"))
    await sim.tick("EURUSD", "1.09960", "1.09970")
    assert sim.fills(cid) == []
    await sim.tick("EURUSD", "1.09940", "1.09950")
    assert sim.fills(cid)[0].fill.price == D("1.09938")


async def test_stop_limit_triggers_then_rests_as_limit(sim: Sim) -> None:
    cid, _ = await sim.order(
        Side.BUY, order_type=OrderType.STOP_LIMIT, stop_price=D("1.10100"), limit_price=D("1.10080")
    )
    await sim.tick("EURUSD", "1.10100", "1.10110")  # stop triggered, ask above the limit
    assert sim.fills(cid) == []
    await sim.tick("EURUSD", "1.10060", "1.10070")
    assert sim.fills(cid)[0].fill.price == D("1.10070")


async def test_modify_and_cancel_pending(sim: Sim) -> None:
    cid, oid = await sim.order(Side.SELL, order_type=OrderType.LIMIT, limit_price=D("1.10500"))
    await sim.broker.modify_order(oid, OrderChanges(limit_price=D("1.10400"), sl=D("1.10600")))
    order = (await sim.broker.open_orders())[0]
    assert (order.request.limit_price, order.request.sl) == (D("1.10400"), D("1.10600"))
    await sim.broker.modify_order(oid, OrderChanges(remove_sl=True))
    assert (await sim.broker.open_orders())[0].request.sl is None
    with pytest.raises(InvalidPrice):
        await sim.broker.modify_order(oid, OrderChanges(limit_price=D("1.09000")))  # sell limit below bid
    with pytest.raises(OrderRejected):
        await sim.broker.modify_order(oid, OrderChanges())
    await sim.broker.cancel_order(oid)
    await settle()
    assert sim.statuses(cid)[-1] is OrderStatus.CANCELLED
    with pytest.raises(OrderNotFound):
        await sim.broker.cancel_order(oid)


async def test_gtd_and_day_expiry(sim: Sim) -> None:
    gtd, _ = await sim.order(
        Side.BUY, order_type=OrderType.LIMIT, limit_price=D("1.09000"),
        time_in_force=TimeInForce.GTD, expiry=MONDAY + timedelta(minutes=5),
    )  # fmt: skip
    day, _ = await sim.order(
        Side.BUY, order_type=OrderType.LIMIT, limit_price=D("1.09000"), time_in_force=TimeInForce.DAY
    )
    await sim.tick("EURUSD", "1.10000", "1.10010", advance=timedelta(minutes=10))
    assert sim.statuses(gtd)[-1] is OrderStatus.EXPIRED
    assert sim.statuses(day)[-1] is OrderStatus.WORKING
    await sim.tick("EURUSD", "1.10000", "1.10010", advance=timedelta(hours=16))  # past midnight UTC
    assert sim.statuses(day)[-1] is OrderStatus.EXPIRED


# ------------------------------------------------------------------ SL / TP / partial close


async def test_stop_loss_and_take_profit(sim: Sim) -> None:
    long_cid, _ = await sim.order(Side.BUY, sl=D("1.09900"), tp=D("1.10500"))
    short_cid, _ = await sim.order(Side.SELL, sl=D("1.10500"), tp=D("1.09900"))
    await sim.tick("EURUSD", "1.09890", "1.09900")  # long SL (bid <= 1.099) and short TP (ask <= 1.099)
    closes = [e.fill for e in sim.rec.of(FillEvent) if e.fill.client_order_id is None]
    assert {c.price for c in closes} == {D("1.09890"), D("1.09900")}
    assert sim.position_changes().count(PositionChange.CLOSED) == 2
    long_pnl = next(c for c in closes if c.side is Side.SELL).realized_pnl
    assert long_pnl == (D("1.09890") - D("1.10010")) * 100_000
    del long_cid, short_cid


async def test_partial_close(sim: Sim) -> None:
    cid, _ = await sim.order(Side.BUY, "0.30")
    pid = sim.fills(cid)[0].fill.position_id
    with pytest.raises(InvalidVolume):
        await sim.broker.close_position(pid, D("0.295"))
    with pytest.raises(InvalidVolume):
        await sim.broker.close_position(pid, D("0.40"))
    await sim.broker.close_position(pid, D("0.10"))
    await settle()
    assert (await sim.broker.positions())[0].volume == D("0.20")
    assert sim.position_changes()[-1] is PositionChange.PARTIALLY_CLOSED


async def test_modify_position_validates_sides(sim: Sim) -> None:
    cid, _ = await sim.order(Side.BUY)
    pid = sim.fills(cid)[0].fill.position_id
    with pytest.raises(InvalidPrice):
        await sim.broker.modify_position(pid, sl=D("1.10100"), tp=None)
    await sim.broker.modify_position(pid, sl=D("1.09000"), tp=None)
    assert (await sim.broker.positions())[0].sl == D("1.09000")
    await sim.broker.modify_position(pid, sl=None, tp=None)
    assert (await sim.broker.positions())[0].sl is None


# ------------------------------------------------------------------ netting


async def test_netting_reduce_close_and_reverse() -> None:
    sim = await make_sim(PaperSettings(hedging=False))
    assert sim.broker.capabilities.netting
    assert not sim.broker.capabilities.hedging
    await sim.order(Side.BUY, "1")
    await sim.order(Side.BUY, "1")  # adds: weighted entry unchanged at the same price
    positions = await sim.broker.positions()
    assert [(p.side, p.volume, p.entry_price) for p in positions] == [(Side.BUY, D(2), D("1.10010"))]

    await sim.order(Side.SELL, "0.5")
    assert (await sim.broker.positions())[0].volume == D("1.5")
    await sim.order(Side.SELL, "2")  # closes 1.5, opens 0.5 short
    positions = await sim.broker.positions()
    assert [(p.side, p.volume) for p in positions] == [(Side.SELL, D("0.5"))]
    assert sim.position_changes()[-2:] == [PositionChange.CLOSED, PositionChange.OPENED]


# ------------------------------------------------------------------ swaps, FX


async def test_swap_rollover_and_triple_wednesday() -> None:
    sim = await make_sim(PaperSettings(swap_long_per_lot=D(-2), swap_short_per_lot=D(1)))
    await sim.order(Side.BUY, "1")
    await sim.order(Side.SELL, "1")
    # Monday 08:00 -> Wednesday 22:00 crosses Mon 21:00, Tue 21:00, Wed 21:00 (x3) = 5 nights
    await sim.tick("EURUSD", "1.10000", "1.10010", advance=timedelta(days=2, hours=14))
    swaps = {p.side: p.swap for p in await sim.broker.positions()}
    assert swaps == {Side.BUY: D(-10), Side.SELL: D(5)}


async def test_quote_currency_conversion_for_jpy_pair() -> None:
    sim = await make_sim(instruments=[eurusd(), usdjpy()])
    await sim.tick("USDJPY", "150.000", "150.010")
    cid, _ = await sim.order(Side.BUY, "1", symbol="USDJPY")
    pid = sim.fills(cid)[0].fill.position_id
    await sim.tick("USDJPY", "151.010", "151.020")
    await sim.broker.close_position(pid)
    await settle()
    pnl = sim.fills()[-1].fill.realized_pnl
    assert pnl == D(100_000) / D("151.015")  # 1 000 pips of 1 lot = 100 000 JPY, converted at the mid


async def test_gold_margin_in_quote_currency() -> None:
    sim = await make_sim(instruments=[eurusd(), xauusd()])
    await sim.tick("XAUUSD", "2400.00", "2400.30")
    await sim.order(Side.BUY, "1", symbol="XAUUSD")
    account = await sim.broker.account()
    assert account.margin == D(100) * D("2400.15") / 100


# ------------------------------------------------------------------ connection gating, events, candles


async def test_no_trade_events_while_down_or_before_snapshot(sim: Sim) -> None:
    cid, _ = await sim.order(Side.BUY, sl=D("1.09900"))
    pid = sim.fills(cid)[0].fill.position_id
    await sim.broker.simulate_disconnect()
    mark = sim.rec.mark
    await sim.tick("EURUSD", "1.09800", "1.09810")  # SL hits broker-side while we are offline
    await sim.broker.simulate_reconnect()
    await sim.tick("EURUSD", "1.09800", "1.09810")
    after = sim.rec.events[mark:]
    assert not any(isinstance(e, FillEvent | PositionEvent | OrderEvent | AccountEvent) for e in after)
    assert any(isinstance(e, TickEvent) for e in after)  # market data is not gated after reconnect

    snap = await sim.broker.snapshot()
    assert pid not in {p.position_id for p in snap.positions}  # reconcile reveals the SL close
    await sim.order(Side.BUY)
    assert sim.rec.of(FillEvent, after=mark)  # events flow again


async def test_events_single_consumer(sim: Sim) -> None:
    with pytest.raises(RuntimeError, match="single consumer"):
        await anext(aiter(sim.broker.events()))


async def test_candles_delegate_to_source(sim: Sim) -> None:
    bars = await sim.broker.candles("EURUSD", Timeframe.M5, MONDAY - timedelta(hours=1), MONDAY)
    assert len(bars) == 12
    assert bars[0].start == MONDAY - timedelta(hours=1)
    with pytest.raises(ValueError, match="before"):
        await sim.broker.candles("EURUSD", Timeframe.M5, MONDAY, MONDAY)


async def test_capabilities_cover_everything_the_oms_cannot_emulate(sim: Sim) -> None:
    caps = sim.broker.capabilities
    assert caps.order_types == frozenset(OrderType)
    assert caps.partial_close
    assert caps.native_sl_tp
    assert caps.modify_pending
    assert caps.server_side_expiry
    assert not caps.sync_order_result
