from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from tests.factories import eurusd, intent
from trader.analytics.execution import ExecutionTracker, percentile, record_of, stats
from trader.analytics.journal import TradeJournal, TradeRecord
from trader.analytics.metrics import compute
from trader.analytics.reports import daily_report, write_report
from trader.domain import (
    OrderRetryEvent,
    OrderStatus,
    OrderType,
    Position,
    PositionChange,
    PositionEvent,
    Side,
    Tick,
)
from trader.domain.models import Fill, Order, StatusChange
from trader.portfolio.ledger import FillLedger, LedgerEntry

D = Decimal
T0 = datetime(2026, 1, 6, 10, 0, tzinfo=UTC)
INST = {"EURUSD": eurusd()}


def fill(
    fid: str, side: Side, vol: str, price: str, *, t: int, pnl: str | None = None, cid: str | None = None
) -> Fill:
    return Fill(
        fill_id=fid,
        client_order_id=cid,
        broker_order_id="O" + fid,
        position_id="P1",
        symbol="EURUSD",
        side=side,
        volume=D(vol),
        price=D(price),
        ts=T0 + timedelta(minutes=t),
        commission=D("-0.30"),
        realized_pnl=None if pnl is None else D(pnl),
    )


def tick(bid: str, t: int = 0) -> Tick:
    ts = T0 + timedelta(minutes=t)
    return Tick(symbol="EURUSD", bid=D(bid), ask=D(bid) + D("0.0001"), ts=ts, recv_ts=ts, recv_mono_ns=0)


def order(
    *,
    side: Side = Side.BUY,
    signal: str = "1.10000",
    fill_price: str | None = "1.10002",
    status: OrderStatus = OrderStatus.FILLED,
    sl: str | None = "1.09800",
    ms: tuple[int, int, int] = (5, 40, 90),
    order_type: OrderType = OrderType.MARKET,
    limit: str | None = None,
) -> Order:
    req = intent(
        side=side,
        sl=None if sl is None else D(sl),
        signal_price=D(signal),
        signal_ts=T0,
        strategy_id="ema",
        strategy_version="3",
        tags=("a",),
        order_type=order_type,
        limit_price=None if limit is None else D(limit),
    ).to_request("C1-test")
    send, ack, filled = (T0 + timedelta(milliseconds=m) for m in ms)
    hist = [
        StatusChange(status=OrderStatus.NEW, ts=T0),
        StatusChange(status=OrderStatus.PENDING_SUBMIT, ts=send),
        StatusChange(status=OrderStatus.ACCEPTED, ts=ack),
    ]
    if status is OrderStatus.FILLED:
        hist.append(StatusChange(status=OrderStatus.FILLED, ts=filled))
        return Order(
            request=req,
            status=status,
            history=tuple(hist),
            filled_volume=req.volume,
            avg_fill_price=D(fill_price or "0"),
        )
    hist.append(StatusChange(status=status, ts=filled, reason="no money"))
    return Order(request=req, status=status, history=tuple(hist), rejection_reason="no money")


def journal(tmp_path: Path | None = None, orders: dict[str, Order] | None = None) -> TradeJournal:
    return TradeJournal(
        "acct",
        currency=lambda: "USD",
        instruments=lambda: INST,
        order_lookup=lambda cid: (orders or {}).get(cid),
        params_hash={"ema": "h1"},
        directory=tmp_path,
    )


def entry(f: Fill) -> LedgerEntry:
    return LedgerEntry(fill=f, strategy_id="ema", day=f.ts.date())


def test_round_trip_with_scale_in_partial_exit_excursions_and_r(tmp_path: Path) -> None:
    j = journal(tmp_path, {"C1-test": order()})
    j.on_entry(entry(fill("1", Side.BUY, "0.10", "1.10000", t=0, cid="C1-test")))
    j.on_tick(tick("1.09950", 1))  # 5 pips against
    j.on_entry(entry(fill("2", Side.BUY, "0.10", "1.10100", t=2)))  # scale in: VWAP 1.10050
    pos = Position(
        position_id="P1",
        symbol="EURUSD",
        side=Side.BUY,
        volume=D("0.2"),
        entry_price=D("1.1005"),
        opened_ts=T0,
        strategy_id="ema",
        sl=D("1.10000"),
    )
    j.on_position(
        PositionEvent(
            account_id="acct", position=pos, change=PositionChange.MODIFIED, ts=T0 + timedelta(minutes=3)
        )
    )
    j.on_tick(tick("1.10350", 4))  # 30 pips for (VWAP 1.10050)
    j.on_entry(entry(fill("3", Side.SELL, "0.05", "1.10300", t=5, pnl="12.50")))
    assert j.open["P1"].volume == D("0.15")
    j.on_entry(entry(fill("4", Side.SELL, "0.15", "1.10200", t=6, pnl="22.50")))
    [t] = j.trades()
    assert (t.entry_price, t.exit_price, t.volume) == (D("1.10050"), D("1.102250"), D("0.20"))
    assert (t.gross, t.commission, t.net) == (D(35), D("-1.20"), D("33.80"))
    assert (t.mfe_price, t.mae_price) == (D("0.00300"), D("0.00050"))  # each vs the entry VWAP at the time
    assert t.initial_sl == D("1.09800")
    assert [x[1] for x in t.sl_history] == [D("1.09800"), D("1.10000")]
    # risk = (1.10050 - 1.09800) x 0.20 lot x 100 000 = 50 USD -> R = 33.80 / 50
    assert t.risk == D(50)
    assert t.r_multiple == D("0.676")
    assert (t.strategy_version, t.params_hash, t.tags) == ("3", "h1", ("a",))
    j.add_note("P1", "late exit")
    j.flush()
    again = journal(tmp_path)
    assert again.load() == 1
    [loaded] = again.trades()
    assert loaded.notes == ("late exit",)
    assert loaded.to_json() == t.to_json()
    assert TradeRecord.from_json(t.to_json()) == t


def test_execution_records_slippage_latency_and_rates(tmp_path: Path) -> None:
    buy = record_of(order(), account_id="a", broker="paper", instrument=eurusd(), rate=D(1))
    # paid 0.2 pip more on 0.10 lot: 0.00002 x 10 000 units = 0.20 USD, adverse (positive)
    assert (buy.slippage_points, buy.slippage_money) == (D(2), D("0.20"))
    sell = record_of(
        order(side=Side.SELL, sl=None), account_id="a", broker="paper", instrument=eurusd(), rate=D(1)
    )
    assert sell.slippage_points == D(-2)  # sold higher: improvement
    limit = record_of(
        order(order_type=OrderType.LIMIT, limit="1.10010", fill_price="1.10005"),
        account_id="a",
        broker="paper",
        instrument=eurusd(),
        rate=D(1),
    )
    assert limit.slippage_points == D(-5)  # vs the limit price
    assert [buy.latency_ms(leg) for leg in ("signal_to_send", "send_to_ack", "ack_to_fill", "total")] == [
        5,
        35,
        50,
        90,
    ]

    tracker = ExecutionTracker(
        "a", "paper", currency=lambda: "USD", instruments=lambda: INST, path=tmp_path / "ex.jsonl"
    )
    risk_reject = Order(
        request=order().request,
        status=OrderStatus.REJECTED,
        history=(StatusChange(status=OrderStatus.REJECTED, ts=T0),),
        rejection_reason="risk:x",
    )
    tracker.on_order(risk_reject)
    assert tracker.records == []  # never sent: not an execution
    tracker.on_retry(
        OrderRetryEvent(account_id="a", client_order_id="C1-test", attempt=1, error="Requote", ts=T0)
    )
    tracker.on_order(order())
    tracker.on_order(order())  # duplicate terminal event: recorded once
    assert len(tracker.records) == 1
    assert tracker.records[0].requotes == 1
    tracker.flush()
    assert (tmp_path / "ex.jsonl").read_text().count("\n") == 1

    rejected = record_of(
        order(status=OrderStatus.REJECTED), account_id="a", broker="ctrader", instrument=eurusd(), rate=D(1)
    )
    grouped = stats([buy, sell, rejected], key=lambda r: (r.broker,))
    paper, ctrader = grouped[("paper",)].to_json(), grouped[("ctrader",)].to_json()
    assert (paper["orders"], paper["fill_rate"], ctrader["reject_rate"]) == (2, 1.0, 1.0)
    assert paper["latency_ms"]["total"] == {"p50": 90.0, "p95": 90.0, "p99": 90.0}  # type: ignore[index]
    assert percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.0
    assert percentile([], 95) is None


def trade(net: str, *, day: int, hours: int = 1, risk: str | None = "10") -> TradeRecord:
    start = T0 + timedelta(days=day)
    return TradeRecord(
        trade_id=f"T{day}{net}",
        account_id="a",
        strategy_id="ema",
        strategy_version="1",
        params_hash="",
        symbol="EURUSD",
        side=Side.BUY,
        volume=D("0.1"),
        entry_price=D(1),
        exit_price=D(1),
        entry_time=start,
        exit_time=start + timedelta(hours=hours),
        gross=D(net),
        commission=D(0),
        swap=D(0),
        initial_sl=None,
        sl_history=(),
        mae_price=D(0),
        mfe_price=D(0),
        mae=None,
        mfe=None,
        risk=None if risk is None else D(risk),
    )


def test_metrics_on_a_known_set() -> None:
    # Tue +10, Wed -5, Thu +20, Fri -5 (one hour each, 2026-01-06 .. 09)
    ts = [trade("10", day=0), trade("-5", day=1), trade("20", day=2), trade("-5", day=3)]
    m = compute(ts, start_equity=D(1000))
    assert (m.trades, m.wins, m.losses, m.win_rate) == (4, 2, 2, 0.5)
    assert (m.net, m.avg_win, m.avg_loss) == (D(20), D(15), D(-5))
    assert m.profit_factor == D(3)
    assert m.expectancy == D(5)
    assert m.avg_r == D("0.5")
    assert m.max_drawdown == D(5)
    assert m.max_drawdown_pct == D(5) / D(1010) * 100
    assert m.recovery_factor == D(4)
    assert (m.longest_win_streak, m.longest_loss_streak) == (1, 1)
    assert m.trades_per_day == 1.0
    assert m.exposure == pytest.approx(4 / (3 * 24 + 1))
    assert m.sharpe is not None
    assert m.sortino is not None
    assert m.sharpe > 0
    empty = compute([])
    assert (empty.trades, empty.win_rate, empty.profit_factor, empty.sharpe) == (0, None, None, None)


def test_daily_report_and_exports(tmp_path: Path) -> None:
    ledger = FillLedger(None)
    ledger.add(fill("1", Side.BUY, "0.10", "1.1", t=0), "ema")
    ledger.add(fill("2", Side.SELL, "0.10", "1.1", t=5, pnl="7"), "ema")
    ex = record_of(order(), account_id="a", broker="paper", instrument=eurusd(), rate=D(1))
    report = daily_report(
        account_id="a",
        day=T0.date(),
        ledger=ledger,
        trades=[trade("6.40", day=0)],
        executions=[ex],
        risk_counts={"approved": 3, "rejected:spread": 1},
        start_equity=D(10_000),
    )
    assert report["pnl"]["realized"]["net"] == "6.40"  # type: ignore[index]
    assert report["trades"]["trades"] == 1  # type: ignore[index]
    assert report["risk"] == {"approved": 3, "rejected:spread": 1}
    assert report["execution"]["overall"]["orders"] == 1  # type: ignore[index]
    paths = write_report(tmp_path, report, [trade("6.40", day=0)], [ex])
    assert [p.name for p in paths] == [
        "2026-01-06.json",
        "2026-01-06-trades.csv",
        "2026-01-06-executions.csv",
    ]
    assert "slippage_points" in (tmp_path / "2026-01-06-executions.csv").read_text().splitlines()[0]
