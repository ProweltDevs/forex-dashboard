from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from hypothesis import given
from hypothesis import strategies as st

from tests.factories import eurusd
from trader.domain import Position, Side, Tick
from trader.domain.models import AccountState, Fill
from trader.portfolio.book import PortfolioBook
from trader.portfolio.ledger import FillLedger, PnL, fill_from_json, fill_to_json

D = Decimal
T0 = datetime(2026, 1, 6, 21, 30, tzinfo=UTC)
USDJPY = eurusd(
    symbol="USDJPY",
    broker_symbol="USDJPY",
    digits=3,
    tick_size=D("0.001"),
    base_currency="USD",
    quote_currency="JPY",
    margin_currency="USD",
)


def fill(
    fid: str,
    *,
    pid: str = "P1",
    side: Side = Side.BUY,
    pnl: str | None = None,
    comm: str = "-0.30",
    swap: str = "0",
    ts: datetime = T0,
    symbol: str = "EURUSD",
    cid: str | None = None,
) -> Fill:
    return Fill(
        fill_id=fid,
        client_order_id=cid,
        broker_order_id="O" + fid,
        position_id=pid,
        symbol=symbol,
        side=side,
        volume=D("0.10"),
        price=D("1.10000"),
        ts=ts,
        commission=D(comm),
        swap=D(swap),
        realized_pnl=None if pnl is None else D(pnl),
    )


def tick(symbol: str, bid: str, ask: str) -> Tick:
    return Tick(symbol=symbol, bid=D(bid), ask=D(ask), ts=T0, recv_ts=T0, recv_mono_ns=0)


def account(balance: str = "10000", equity: str | None = None) -> AccountState:
    b = D(balance)
    e = D(equity) if equity else b
    return AccountState(
        account_id="a", currency="USD", balance=b, equity=e, margin=D(0), free_margin=e, leverage=100, ts=T0
    )


def test_ledger_dedupes_attributes_and_persists(tmp_path: Path) -> None:
    path = tmp_path / "fills.jsonl"
    ledger = FillLedger(path, day_offset_minutes=-120)  # trading day starts 22:00 UTC
    assert ledger.add(fill("1"), "ema") is not None
    assert ledger.add(fill("1"), "ema") is None  # live + backfill of the same deal: counted once
    ledger.add(fill("2", pnl="12.50", swap="-0.40", ts=T0 + timedelta(hours=1)), "ema")  # 22:30: next day
    ledger.add(fill("3", pid="P2", symbol="USDJPY", pnl="-3", ts=T0 + timedelta(hours=1)), "manual")
    day1, day2 = T0.date(), (T0 + timedelta(days=1)).date()
    assert ledger.days() == [day1, day2]
    assert ledger.realized(day=day1) == PnL(commission=D("-0.30"), deals=1)
    assert ledger.realized(day=day2).net == D("12.50") - D("0.30") - D("0.40") + D(-3) - D("0.30")
    assert ledger.attribution(day2)[("ema", "EURUSD")].net == D("11.80")
    assert ledger.realized(strategy_id="manual").gross == D(-3)
    ledger.append_lines(ledger.take_pending())
    again = FillLedger(path, day_offset_minutes=-120)
    assert again.load() == 3
    assert [e.strategy_id for e in again.entries()] == ["ema", "ema", "manual"]
    f = fill("9", pnl="1.23", cid="C9")
    assert fill_from_json(fill_to_json(f)) == f


def test_book_prices_at_the_exit_side_converts_and_tracks_high_water() -> None:
    ledger = FillLedger(None)
    book = PortfolioBook("a", ledger)
    book.on_account(account())
    long = Position(
        position_id="L",
        symbol="EURUSD",
        side=Side.BUY,
        volume=D("1"),
        entry_price=D("1.10000"),
        opened_ts=T0,
        strategy_id="s",
    )
    short = Position(
        position_id="S",
        symbol="USDJPY",
        side=Side.SELL,
        volume=D("0.5"),
        entry_price=D("150.000"),
        opened_ts=T0,
        strategy_id="s",
    )
    book.set_positions((long, short), {"EURUSD": eurusd(), "USDJPY": USDJPY})
    book.on_tick(tick("EURUSD", "1.10100", "1.10110"))
    assert book.unrealized_of(long) == D("100.00000")  # (bid 1.10100 - 1.10000) x 100 000
    assert book.unrealized_of(short) is None  # no USDJPY tick yet
    assert book.unrealized() == (D("100.00000"), 1)
    book.on_tick(tick("USDJPY", "149.490", "149.510"))
    # short exits at the ask: (150.000 - 149.510) x 50 000 JPY = 24 500 JPY, / mid 149.500 = 163.88 USD
    assert book.unrealized_of(short) == D(24_500) / D("149.500")
    p1 = book.sample(T0)
    assert p1 is not None
    assert p1.high_water == p1.equity == D(10_000) + D(100) + D(24_500) / D("149.500")
    book.on_tick(tick("EURUSD", "1.09900", "1.09910"))
    p2 = book.sample(T0 + timedelta(seconds=1))
    assert p2 is not None
    assert (p2.high_water, p2.drawdown) == (p1.high_water, D(200))
    ledger.add(fill("c", pnl="50", comm="-1", ts=T0 + timedelta(seconds=2)), "s")  # after the baseline
    assert book.computed_balance() == D(10_049)
    book.on_account(account("10049"))
    assert book.balance_gap() == 0


@given(
    st.decimals(min_value=D("0.5"), max_value=D("2"), places=5),
    st.decimals(min_value=D("0.5"), max_value=D("2"), places=5),
    st.decimals(min_value=D("0.01"), max_value=D("10"), places=2),
)
def test_unrealized_is_symmetric_between_long_and_short(
    entry: Decimal, mid: Decimal, volume: Decimal
) -> None:
    book = PortfolioBook("a", FillLedger(None))
    book.on_account(account())
    long = Position(
        position_id="L",
        symbol="EURUSD",
        side=Side.BUY,
        volume=volume,
        entry_price=entry,
        opened_ts=T0,
        strategy_id="s",
    )
    short = Position(
        position_id="S",
        symbol="EURUSD",
        side=Side.SELL,
        volume=volume,
        entry_price=entry,
        opened_ts=T0,
        strategy_id="s",
    )
    book.set_positions((long, short), {"EURUSD": eurusd()})
    book.on_tick(Tick(symbol="EURUSD", bid=mid, ask=mid, ts=T0, recv_ts=T0, recv_mono_ns=0))
    lu, su = book.unrealized_of(long), book.unrealized_of(short)
    assert lu is not None
    assert su is not None
    assert lu == -su == (mid - entry) * volume * 100_000
