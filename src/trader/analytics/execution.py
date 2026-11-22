"""Execution quality (SPEC §10, top priority): one record per order and stats per broker/symbol/hour.

For every order: signal time and price (from the intent), send time (PENDING_SUBMIT), ACK time (ACCEPTED),
first fill time and average fill price, final status, rejection reason and requote/retry count.

- **Slippage** vs the reference price (the intent's signal price for market orders, the limit/stop price
  otherwise), side-adjusted: **positive = adverse** (paid more / sold lower), negative = improvement. In
  points (ticks) and money (account currency, at the fill's conversion rate).
- **Latencies** (ms, OMS clock): signal->send, send->ack, ack->fill and signal->fill (total).
- Stats: count, fill/reject/requote rates, slippage mean/p50/p95 and latency p50/p95/p99, grouped by any
  key (default broker, symbol, UTC hour), so brokers can be compared side by side.

Storage: finished orders in ``{data_dir}/executions/{account}.jsonl``.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from trader.domain.enums import OrderStatus, OrderType
from trader.domain.errors import ConversionUnavailable
from trader.domain.events import OrderRetryEvent
from trader.domain.fx import conversion_rate
from trader.domain.models import Instrument, Order, Tick
from trader.domain.time import from_epoch_ms, to_epoch_ms
from trader.portfolio.service import Listener

from .types import Out, OutObj

ZERO = Decimal(0)
_FILLED = (OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED)


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionRecord:
    client_order_id: str
    account_id: str
    broker: str
    strategy_id: str
    symbol: str
    side: str
    order_type: str
    volume: Decimal
    filled_volume: Decimal
    status: str
    rejection_reason: str | None
    signal_ts: datetime | None
    send_ts: datetime | None
    ack_ts: datetime | None
    fill_ts: datetime | None
    reference_price: Decimal | None
    fill_price: Decimal | None
    slippage_points: Decimal | None
    slippage_money: Decimal | None
    retries: int
    requotes: int

    @property
    def hour(self) -> int:
        ts = self.send_ts or self.signal_ts
        return -1 if ts is None else ts.hour

    def latency_ms(self, leg: str) -> float | None:
        a, b = {
            "signal_to_send": (self.signal_ts, self.send_ts),
            "send_to_ack": (self.send_ts, self.ack_ts),
            "ack_to_fill": (self.ack_ts, self.fill_ts),
            "total": (self.signal_ts, self.fill_ts),
        }[leg]
        return None if a is None or b is None else (b - a).total_seconds() * 1000

    def to_json(self) -> OutObj:
        def s(v: object) -> str | None:
            return None if v is None else str(v)

        def ms(v: datetime | None) -> int | None:
            return None if v is None else to_epoch_ms(v)

        out: OutObj = {
            "client_order_id": self.client_order_id,
            "account_id": self.account_id,
            "broker": self.broker,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "volume": str(self.volume),
            "filled_volume": str(self.filled_volume),
            "status": self.status,
            "rejection_reason": self.rejection_reason,
            "signal_ts_ms": ms(self.signal_ts),
            "send_ts_ms": ms(self.send_ts),
            "ack_ts_ms": ms(self.ack_ts),
            "fill_ts_ms": ms(self.fill_ts),
            "reference_price": s(self.reference_price),
            "fill_price": s(self.fill_price),
            "slippage_points": s(self.slippage_points),
            "slippage_money": s(self.slippage_money),
            "retries": self.retries,
            "requotes": self.requotes,
        }
        for leg in LEGS:
            value = self.latency_ms(leg)
            out[f"{leg}_ms"] = None if value is None else round(value, 3)
        return out

    @classmethod
    def from_json(cls, d: Mapping[str, object]) -> ExecutionRecord:
        def od(v: object) -> Decimal | None:
            return None if v is None else Decimal(str(v))

        def ts(v: object) -> datetime | None:
            return None if v is None else from_epoch_ms(int(str(v)))

        reason = d.get("rejection_reason")
        return cls(
            client_order_id=str(d["client_order_id"]),
            account_id=str(d["account_id"]),
            broker=str(d["broker"]),
            strategy_id=str(d["strategy_id"]),
            symbol=str(d["symbol"]),
            side=str(d["side"]),
            order_type=str(d["order_type"]),
            volume=Decimal(str(d["volume"])),
            filled_volume=Decimal(str(d["filled_volume"])),
            status=str(d["status"]),
            rejection_reason=None if reason is None else str(reason),
            signal_ts=ts(d.get("signal_ts_ms")),
            send_ts=ts(d.get("send_ts_ms")),
            ack_ts=ts(d.get("ack_ts_ms")),
            fill_ts=ts(d.get("fill_ts_ms")),
            reference_price=od(d.get("reference_price")),
            fill_price=od(d.get("fill_price")),
            slippage_points=od(d.get("slippage_points")),
            slippage_money=od(d.get("slippage_money")),
            retries=int(str(d.get("retries") or 0)),
            requotes=int(str(d.get("requotes") or 0)),
        )


LEGS = ("signal_to_send", "send_to_ack", "ack_to_fill", "total")


def _first(order: Order, statuses: Iterable[OrderStatus]) -> datetime | None:
    wanted = set(statuses)
    return next((h.ts for h in order.history if h.status in wanted), None)


def record_of(
    order: Order,
    *,
    account_id: str,
    broker: str,
    instrument: Instrument | None,
    rate: Decimal | None,
    retries: int = 0,
    requotes: int = 0,
) -> ExecutionRecord:
    req = order.request
    reference = (
        req.signal_price if req.order_type is OrderType.MARKET else (req.limit_price or req.stop_price)
    )
    fill = order.avg_fill_price
    points = money = None
    if reference is not None and fill is not None and instrument is not None:
        adverse = req.side.sign * (fill - reference)
        points = adverse / instrument.tick_size
        if rate is not None:
            money = adverse * order.filled_volume * instrument.contract_size * rate
    return ExecutionRecord(
        client_order_id=order.client_order_id,
        account_id=account_id,
        broker=broker,
        strategy_id=req.strategy_id,
        symbol=req.symbol,
        side=req.side.value,
        order_type=req.order_type.value,
        volume=req.volume,
        filled_volume=order.filled_volume,
        status=order.status.value,
        rejection_reason=order.rejection_reason,
        signal_ts=req.signal_ts,
        send_ts=_first(order, (OrderStatus.PENDING_SUBMIT,)),
        ack_ts=_first(order, (OrderStatus.ACCEPTED,)),
        fill_ts=_first(order, _FILLED),
        reference_price=reference,
        fill_price=fill,
        slippage_points=points,
        slippage_money=money,
        retries=retries,
        requotes=requotes,
    )


class ExecutionTracker(Listener):
    def __init__(
        self,
        account_id: str,
        broker: str,
        *,
        currency: Callable[[], str | None],
        instruments: Callable[[], Mapping[str, Instrument]],
        path: Path | None = None,
    ) -> None:
        self.account_id, self.broker = account_id, broker
        self._currency, self._instruments = currency, instruments
        self.path = path
        self.records: list[ExecutionRecord] = []
        self._retries: dict[str, list[str]] = {}
        self._ticks: dict[str, Tick] = {}
        self._done: set[str] = set()
        self._pending: list[str] = []

    def on_tick(self, tick: Tick) -> None:
        self._ticks[tick.symbol] = tick

    def on_retry(self, event: OrderRetryEvent) -> None:
        self._retries.setdefault(event.client_order_id, []).append(event.error)

    def on_order(self, order: Order) -> None:
        if not order.status.is_terminal or order.client_order_id in self._done:
            return
        if not any(h.status is OrderStatus.PENDING_SUBMIT for h in order.history) and not order.filled_volume:
            return  # rejected by risk/OMS before it was sent: not an execution
        self._done.add(order.client_order_id)
        inst = self._instruments().get(order.request.symbol)
        retries = self._retries.pop(order.client_order_id, [])
        rec = record_of(
            order,
            account_id=self.account_id,
            broker=self.broker,
            instrument=inst,
            rate=self._rate(inst),
            retries=len(retries),
            requotes=sum(1 for e in retries if e == "Requote"),
        )
        self.records.append(rec)
        self._pending.append(json.dumps(rec.to_json(), separators=(",", ":")))

    def _rate(self, inst: Instrument | None) -> Decimal | None:
        ccy = self._currency()
        if inst is None or ccy is None:
            return None
        try:
            return conversion_rate(inst.quote_currency, ccy, self._ticks.get)
        except ConversionUnavailable:
            return None

    def flush(self) -> None:
        if self.path is None or not self._pending:
            return
        lines, self._pending = self._pending, []
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")


# ------------------------------------------------------------------------------------------------ stats


def percentile(values: Sequence[float], q: float) -> float | None:
    """Nearest-rank percentile (q in 0..100)."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]


@dataclass(slots=True)
class ExecutionStats:
    orders: int = 0
    filled: int = 0
    rejected: int = 0
    requoted: int = 0  # orders with at least one requote
    slippage_points: list[float] = field(default_factory=list)
    slippage_money: Decimal = ZERO
    latencies: dict[str, list[float]] = field(default_factory=lambda: {leg: [] for leg in LEGS})

    def add(self, r: ExecutionRecord) -> None:
        self.orders += 1
        self.filled += r.filled_volume > 0
        self.rejected += r.status == OrderStatus.REJECTED.value
        self.requoted += r.requotes > 0
        if r.slippage_points is not None:
            self.slippage_points.append(float(r.slippage_points))
        if r.slippage_money is not None:
            self.slippage_money += r.slippage_money
        for leg in LEGS:
            value = r.latency_ms(leg)
            if value is not None:
                self.latencies[leg].append(value)

    def to_json(self) -> OutObj:
        def rate(n: int) -> float | None:
            return None if not self.orders else round(n / self.orders, 4)

        slip = self.slippage_points
        lat: dict[str, Out] = {}
        for leg, values in self.latencies.items():
            lat[leg] = {f"p{q}": _r(percentile(values, q)) for q in (50, 95, 99)}
        return {
            "orders": self.orders,
            "fill_rate": rate(self.filled),
            "reject_rate": rate(self.rejected),
            "requote_rate": rate(self.requoted),
            "slippage_points": {
                "mean": _r(sum(slip) / len(slip) if slip else None),
                "p50": _r(percentile(slip, 50)),
                "p95": _r(percentile(slip, 95)),
            },
            "slippage_money": str(self.slippage_money.quantize(Decimal("0.01"))),
            "latency_ms": lat,
        }


def _r(v: float | None) -> float | None:
    return None if v is None else round(v, 3)


def stats(
    records: Iterable[ExecutionRecord],
    key: Callable[[ExecutionRecord], tuple[object, ...]] = lambda r: (r.broker, r.symbol, r.hour),
) -> dict[tuple[object, ...], ExecutionStats]:
    out: dict[tuple[object, ...], ExecutionStats] = {}
    for r in records:
        out.setdefault(key(r), ExecutionStats()).add(r)
    return out
