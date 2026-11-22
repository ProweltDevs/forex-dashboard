"""JSON codec for OMS state (orders, requests, positions, exit specs). Decimals as strings, datetimes as
epoch-ms (CLAUDE.md storage rule), enums by value. Round-trip exact; covered by property tests."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from decimal import Decimal

from trader.domain.enums import OrderStatus, OrderType, Side, TimeInForce
from trader.domain.exits import BreakEven, ExitSpec, ScaleLevel, ScaleOut, TimeExit, TrailingStop
from trader.domain.models import Order, OrderRequest, Position, StatusChange
from trader.domain.time import from_epoch_ms, to_epoch_ms

type JsonValue = str | int | bool | list[JsonValue] | dict[str, JsonValue] | None
type JsonObj = dict[str, JsonValue]


def _d(v: Decimal | None) -> str | None:
    return None if v is None else str(v)


def _pd(v: JsonValue) -> Decimal | None:
    return None if v is None else Decimal(str(v))


def _req_d(v: JsonValue) -> Decimal:
    if v is None:
        raise ValueError("missing decimal")
    return Decimal(str(v))


def _t(v: datetime | None) -> int | None:
    return None if v is None else to_epoch_ms(v)


def _pt(v: JsonValue) -> datetime | None:
    return None if v is None else from_epoch_ms(int(str(v)))


def _req_t(v: JsonValue) -> datetime:
    if v is None:
        raise ValueError("missing timestamp")
    return from_epoch_ms(int(str(v)))


def _s(v: JsonValue) -> str:
    return "" if v is None else str(v)


# ------------------------------------------------------------------------------------------- exits


def exit_to_json(spec: ExitSpec) -> JsonObj:
    if isinstance(spec, TrailingStop):
        return {
            "type": "trailing",
            "distance": str(spec.distance),
            "activation": str(spec.activation),
            "step": str(spec.step),
        }
    if isinstance(spec, BreakEven):
        return {"type": "break_even", "trigger": str(spec.trigger), "offset": str(spec.offset)}
    if isinstance(spec, TimeExit):
        return {"type": "time_exit", "after_ms": spec.after // timedelta(milliseconds=1)}
    return {
        "type": "scale_out",
        "levels": [{"distance": str(lv.distance), "fraction": str(lv.fraction)} for lv in spec.levels],
    }


def exit_from_json(data: Mapping[str, JsonValue]) -> ExitSpec:
    kind = data["type"]
    if kind == "trailing":
        return TrailingStop(
            distance=_req_d(data["distance"]),
            activation=_req_d(data["activation"]),
            step=_req_d(data["step"]),
        )
    if kind == "break_even":
        return BreakEven(trigger=_req_d(data["trigger"]), offset=_req_d(data["offset"]))
    if kind == "time_exit":
        return TimeExit(after=timedelta(milliseconds=int(str(data["after_ms"]))))
    if kind == "scale_out":
        levels = data["levels"]
        assert isinstance(levels, list)
        return ScaleOut(
            levels=tuple(
                ScaleLevel(distance=_req_d(lv["distance"]), fraction=_req_d(lv["fraction"]))
                for lv in levels
                if isinstance(lv, dict)
            )
        )
    raise ValueError(f"unknown exit spec {kind!r}")


# ------------------------------------------------------------------------------------- requests and orders


def request_to_json(r: OrderRequest) -> JsonObj:
    return {
        "client_order_id": r.client_order_id,
        "symbol": r.symbol,
        "side": r.side.value,
        "order_type": r.order_type.value,
        "volume": str(r.volume),
        "strategy_id": r.strategy_id,
        "strategy_version": r.strategy_version,
        "limit_price": _d(r.limit_price),
        "stop_price": _d(r.stop_price),
        "sl": _d(r.sl),
        "tp": _d(r.tp),
        "time_in_force": r.time_in_force.value,
        "expiry": _t(r.expiry),
        "max_slippage": _d(r.max_slippage),
        "signal_ts": _t(r.signal_ts),
        "signal_price": _d(r.signal_price),
        "tags": list(r.tags),
        "exits": [exit_to_json(e) for e in r.exits],
        "oco_group": r.oco_group,
    }


def request_from_json(d: Mapping[str, JsonValue]) -> OrderRequest:
    tags = d.get("tags") or []
    exits = d.get("exits") or []
    assert isinstance(tags, list)
    assert isinstance(exits, list)
    oco = d.get("oco_group")
    return OrderRequest(
        client_order_id=_s(d["client_order_id"]),
        symbol=_s(d["symbol"]),
        side=Side(_s(d["side"])),
        order_type=OrderType(_s(d["order_type"])),
        volume=_req_d(d["volume"]),
        strategy_id=_s(d["strategy_id"]),
        strategy_version=_s(d["strategy_version"]),
        limit_price=_pd(d.get("limit_price")),
        stop_price=_pd(d.get("stop_price")),
        sl=_pd(d.get("sl")),
        tp=_pd(d.get("tp")),
        time_in_force=TimeInForce(_s(d["time_in_force"])),
        expiry=_pt(d.get("expiry")),
        max_slippage=_pd(d.get("max_slippage")),
        signal_ts=_pt(d.get("signal_ts")),
        signal_price=_pd(d.get("signal_price")),
        tags=tuple(str(t) for t in tags),
        exits=tuple(exit_from_json(e) for e in exits if isinstance(e, dict)),
        oco_group=None if oco is None else str(oco),
    )


def order_to_json(o: Order) -> JsonObj:
    return {
        "request": request_to_json(o.request),
        "status": o.status.value,
        "history": [
            {"status": h.status.value, "ts": to_epoch_ms(h.ts), "reason": h.reason} for h in o.history
        ],
        "broker_order_id": o.broker_order_id,
        "filled_volume": str(o.filled_volume),
        "avg_fill_price": _d(o.avg_fill_price),
        "rejection_reason": o.rejection_reason,
    }


def order_from_json(d: Mapping[str, JsonValue]) -> Order:
    req = d["request"]
    hist = d["history"]
    assert isinstance(req, dict)
    assert isinstance(hist, list)
    bid = d.get("broker_order_id")
    rej = d.get("rejection_reason")
    return Order(
        request=request_from_json(req),
        status=OrderStatus(_s(d["status"])),
        history=tuple(
            StatusChange(
                status=OrderStatus(_s(h["status"])),
                ts=_req_t(h["ts"]),
                reason=None if h.get("reason") is None else str(h["reason"]),
            )
            for h in hist
            if isinstance(h, dict)
        ),
        broker_order_id=None if bid is None else str(bid),
        filled_volume=_req_d(d["filled_volume"]),
        avg_fill_price=_pd(d.get("avg_fill_price")),
        rejection_reason=None if rej is None else str(rej),
    )


def position_to_json(p: Position) -> JsonObj:
    return {
        "position_id": p.position_id,
        "symbol": p.symbol,
        "side": p.side.value,
        "volume": str(p.volume),
        "entry_price": str(p.entry_price),
        "opened_ts": to_epoch_ms(p.opened_ts),
        "strategy_id": p.strategy_id,
        "sl": _d(p.sl),
        "tp": _d(p.tp),
        "swap": str(p.swap),
        "commission": str(p.commission),
    }


def position_from_json(d: Mapping[str, JsonValue]) -> Position:
    return Position(
        position_id=_s(d["position_id"]),
        symbol=_s(d["symbol"]),
        side=Side(_s(d["side"])),
        volume=_req_d(d["volume"]),
        entry_price=_req_d(d["entry_price"]),
        opened_ts=_req_t(d["opened_ts"]),
        strategy_id=_s(d["strategy_id"]),
        sl=_pd(d.get("sl")),
        tp=_pd(d.get("tp")),
        swap=_req_d(d.get("swap", "0")),
        commission=_req_d(d.get("commission", "0")),
    )
