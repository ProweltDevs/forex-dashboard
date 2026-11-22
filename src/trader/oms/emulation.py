"""Emulated orders (SPEC §6): exits and order types the broker does not provide natively.

Pure and deterministic: driven by ticks (``tick.ts`` is the clock) and ``on_time``, it never does IO.
It returns ``Action``s that the OrderManager executes through the broker. State is plain data, serialised
with every change, so emulations survive restarts.

Kinds: trailing stop, break-even, time exit, scale-out ladder, SL/TP (broker without native SL/TP),
order expiry (broker without server-side expiry) and stop-limit (broker without STOP_LIMIT).
OCO groups are order-level and handled by the OrderManager.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta
from decimal import Decimal

from trader.domain.enums import Side
from trader.domain.exits import BreakEven, ExitSpec, ScaleOut, TimeExit, TrailingStop
from trader.domain.jsontypes import JsonObj, JsonValue
from trader.domain.models import Instrument, Position, Tick
from trader.domain.money import floor_to_step
from trader.domain.time import from_epoch_ms, to_epoch_ms

# ------------------------------------------------------------------------------------------- actions


@dataclass(frozen=True, slots=True)
class ModifyStop:
    position_id: str
    sl: Decimal
    reason: str


@dataclass(frozen=True, slots=True)
class ClosePosition:
    position_id: str
    volume: Decimal | None  # None = everything
    reason: str


@dataclass(frozen=True, slots=True)
class CancelOrder:
    client_order_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class SendStopLimit:
    """The stop of an emulated STOP_LIMIT triggered: send its limit leg to the broker."""

    client_order_id: str
    reason: str


type Action = ModifyStop | ClosePosition | CancelOrder | SendStopLimit

# ------------------------------------------------------------------------------------------- state


def _exit_price(side: Side, tick: Tick) -> Decimal:
    """Price the position would close at: bid for longs, ask for shorts."""
    return tick.bid if side is Side.BUY else tick.ask


def _profit(side: Side, entry: Decimal, price: Decimal) -> Decimal:
    return (price - entry) * side.sign


@dataclass(slots=True, kw_only=True)
class Trailing:
    id: str
    position_id: str
    symbol: str
    side: Side
    entry: Decimal
    distance: Decimal
    activation: Decimal
    step: Decimal
    stop: Decimal | None = None  # last SL we asked for
    desired: Decimal | None = None  # better SL waiting for the modify throttle
    last_sent_ms: int | None = None


@dataclass(slots=True, kw_only=True)
class BreakEvenEmu:
    id: str
    position_id: str
    symbol: str
    side: Side
    entry: Decimal
    trigger: Decimal
    offset: Decimal
    done: bool = False


@dataclass(slots=True, kw_only=True)
class TimeExitEmu:
    id: str
    position_id: str
    symbol: str
    deadline: datetime
    done: bool = False


@dataclass(slots=True, kw_only=True)
class ScaleOutEmu:
    id: str
    position_id: str
    symbol: str
    side: Side
    entry: Decimal
    initial_volume: Decimal
    distances: tuple[Decimal, ...]
    fractions: tuple[Decimal, ...]
    next_level: int = 0
    closed: Decimal = Decimal(0)


@dataclass(slots=True, kw_only=True)
class SlTpEmu:
    id: str
    position_id: str
    symbol: str
    side: Side
    sl: Decimal | None
    tp: Decimal | None
    done: bool = False


@dataclass(slots=True, kw_only=True)
class ExpiryEmu:
    id: str
    client_order_id: str
    symbol: str
    deadline: datetime
    done: bool = False


@dataclass(slots=True, kw_only=True)
class StopLimitEmu:
    id: str
    client_order_id: str
    symbol: str
    side: Side
    stop: Decimal
    triggered: bool = False


type Emulation = Trailing | BreakEvenEmu | TimeExitEmu | ScaleOutEmu | SlTpEmu | ExpiryEmu | StopLimitEmu

_KINDS: dict[str, type[Emulation]] = {
    "trailing": Trailing,
    "break_even": BreakEvenEmu,
    "time_exit": TimeExitEmu,
    "scale_out": ScaleOutEmu,
    "sl_tp": SlTpEmu,
    "expiry": ExpiryEmu,
    "stop_limit": StopLimitEmu,
}
_KIND_OF = {cls: name for name, cls in _KINDS.items()}

# ------------------------------------------------------------------------------------------- codec


def _enc(value: object) -> JsonValue:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return to_epoch_ms(value)
    if isinstance(value, Side):
        return value.value
    if isinstance(value, tuple):
        return [_enc(v) for v in value]
    if value is None or isinstance(value, str | int | bool):
        return value
    raise TypeError(f"cannot encode {type(value).__name__}")


def encode(emu: Emulation) -> JsonObj:
    return {"kind": _KIND_OF[type(emu)], **{k: _enc(v) for k, v in asdict(emu).items()}}


def decode(data: Mapping[str, JsonValue]) -> Emulation:
    cls = _KINDS[str(data["kind"])]
    kwargs: dict[str, object] = {}
    for f in fields(cls):
        raw = data.get(f.name)
        typ = str(f.type)
        if raw is None:
            kwargs[f.name] = None
        elif "tuple[Decimal" in typ:
            if not isinstance(raw, list):
                raise ValueError(f"{f.name}: expected a list")
            kwargs[f.name] = tuple(Decimal(str(x)) for x in raw)
        elif "Decimal" in typ:
            kwargs[f.name] = Decimal(str(raw))
        elif "datetime" in typ:
            kwargs[f.name] = from_epoch_ms(int(str(raw)))
        elif typ == "Side":
            kwargs[f.name] = Side(str(raw))
        else:
            kwargs[f.name] = raw
    return cls(**kwargs)  # type: ignore[arg-type]


# ------------------------------------------------------------------------------------------- engine


@dataclass(slots=True)
class EmulationEngine:
    min_modify_interval: timedelta = timedelta(seconds=1)
    items: dict[str, Emulation] = field(default_factory=dict)
    _seq: int = 0

    def _id(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}-{self._seq}"

    def __len__(self) -> int:
        return len(self.items)

    def symbols(self) -> set[str]:
        return {e.symbol for e in self.items.values()}

    def load(self, items: Iterable[Mapping[str, JsonValue]]) -> None:
        for raw in items:
            emu = decode(raw)
            self.items[emu.id] = emu
        self._seq = max(
            [self._seq, *(int(i.rsplit("-", 1)[-1]) for i in self.items if i.rsplit("-", 1)[-1].isdigit())]
        )

    def dump(self) -> list[JsonObj]:
        return [encode(e) for e in self.items.values()]

    # ------------------------------------------------------------------ arming

    def arm_exits(
        self,
        *,
        position_id: str,
        symbol: str,
        side: Side,
        entry: Decimal,
        volume: Decimal,
        opened: datetime,
        exits: Iterable[ExitSpec],
    ) -> list[Emulation]:
        armed: list[Emulation] = []
        for spec in exits:
            emu: Emulation
            if isinstance(spec, TrailingStop):
                emu = Trailing(
                    id=self._id("trail"),
                    position_id=position_id,
                    symbol=symbol,
                    side=side,
                    entry=entry,
                    distance=spec.distance,
                    activation=spec.activation,
                    step=spec.step,
                )
            elif isinstance(spec, BreakEven):
                emu = BreakEvenEmu(
                    id=self._id("be"),
                    position_id=position_id,
                    symbol=symbol,
                    side=side,
                    entry=entry,
                    trigger=spec.trigger,
                    offset=spec.offset,
                )
            elif isinstance(spec, TimeExit):
                emu = TimeExitEmu(
                    id=self._id("time"), position_id=position_id, symbol=symbol, deadline=opened + spec.after
                )
            elif isinstance(spec, ScaleOut):
                emu = ScaleOutEmu(
                    id=self._id("scale"),
                    position_id=position_id,
                    symbol=symbol,
                    side=side,
                    entry=entry,
                    initial_volume=volume,
                    distances=tuple(lv.distance for lv in spec.levels),
                    fractions=tuple(lv.fraction for lv in spec.levels),
                )
            else:  # pragma: no cover - exhaustive over ExitSpec
                raise TypeError(f"unknown exit spec {spec!r}")
            self.items[emu.id] = emu
            armed.append(emu)
        return armed

    def arm_sl_tp(
        self, *, position_id: str, symbol: str, side: Side, sl: Decimal | None, tp: Decimal | None
    ) -> SlTpEmu:
        for emu in self.items.values():
            if isinstance(emu, SlTpEmu) and emu.position_id == position_id:
                emu.sl, emu.tp = sl, tp
                return emu
        new = SlTpEmu(id=self._id("sltp"), position_id=position_id, symbol=symbol, side=side, sl=sl, tp=tp)
        self.items[new.id] = new
        return new

    def arm_expiry(self, *, client_order_id: str, symbol: str, deadline: datetime) -> ExpiryEmu:
        emu = ExpiryEmu(
            id=self._id("expiry"), client_order_id=client_order_id, symbol=symbol, deadline=deadline
        )
        self.items[emu.id] = emu
        return emu

    def arm_stop_limit(self, *, client_order_id: str, symbol: str, side: Side, stop: Decimal) -> StopLimitEmu:
        emu = StopLimitEmu(
            id=self._id("stoplimit"), client_order_id=client_order_id, symbol=symbol, side=side, stop=stop
        )
        self.items[emu.id] = emu
        return emu

    def stop_limit(self, client_order_id: str) -> StopLimitEmu | None:
        return next(
            (
                e
                for e in self.items.values()
                if isinstance(e, StopLimitEmu) and e.client_order_id == client_order_id
            ),
            None,
        )

    def sl_tp(self, position_id: str) -> SlTpEmu | None:
        return next(
            (e for e in self.items.values() if isinstance(e, SlTpEmu) and e.position_id == position_id), None
        )

    # ------------------------------------------------------------------ lifecycle

    def drop_position(self, position_id: str) -> int:
        doomed = [k for k, e in self.items.items() if getattr(e, "position_id", None) == position_id]
        for k in doomed:
            del self.items[k]
        return len(doomed)

    def drop_order(self, client_order_id: str) -> int:
        doomed = [k for k, e in self.items.items() if getattr(e, "client_order_id", None) == client_order_id]
        for k in doomed:
            del self.items[k]
        return len(doomed)

    def note_stop(self, position_id: str, sl: Decimal | None) -> None:
        """The SL changed (by us or externally): trailing/break-even must never move it backwards."""
        for emu in self.items.values():
            if (
                isinstance(emu, Trailing)
                and emu.position_id == position_id
                and sl is not None
                and (emu.stop is None or (sl - emu.stop) * emu.side.sign > 0)
            ):
                emu.stop = sl

    # ------------------------------------------------------------------ evaluation

    def on_tick(
        self, tick: Tick, positions: Mapping[str, Position], instruments: Mapping[str, Instrument]
    ) -> list[Action]:
        actions: list[Action] = []
        now_ms = to_epoch_ms(tick.ts)
        inst = instruments.get(tick.symbol)
        for emu in list(self.items.values()):
            if emu.symbol != tick.symbol:
                continue
            if isinstance(emu, StopLimitEmu):
                if not emu.triggered:
                    hit = tick.ask >= emu.stop if emu.side is Side.BUY else tick.bid <= emu.stop
                    if hit:
                        emu.triggered = True
                        actions.append(SendStopLimit(emu.client_order_id, f"stop {emu.stop} reached"))
                continue
            if isinstance(emu, ExpiryEmu):
                continue
            pos = positions.get(emu.position_id)
            if pos is None:
                continue
            if isinstance(emu, Trailing):
                actions.extend(self._trail(emu, tick, pos, inst, now_ms))
            elif isinstance(emu, BreakEvenEmu):
                if not emu.done and _profit(emu.side, emu.entry, _exit_price(emu.side, tick)) >= emu.trigger:
                    emu.done = True
                    target = emu.entry + emu.offset * emu.side.sign
                    if inst is not None:
                        target = inst.round_price(target)
                    if pos.sl is None or (target - pos.sl) * emu.side.sign > 0:
                        actions.append(ModifyStop(emu.position_id, target, "break-even"))
                        self.note_stop(emu.position_id, target)
            elif isinstance(emu, ScaleOutEmu):
                actions.extend(self._scale(emu, tick, pos, inst))
            elif isinstance(emu, SlTpEmu):
                price = _exit_price(emu.side, tick)
                long = emu.side is Side.BUY
                if not emu.done and emu.sl is not None and (price <= emu.sl if long else price >= emu.sl):
                    emu.done = True
                    actions.append(ClosePosition(emu.position_id, None, f"emulated stop loss {emu.sl}"))
                elif not emu.done and emu.tp is not None and (price >= emu.tp if long else price <= emu.tp):
                    emu.done = True
                    actions.append(ClosePosition(emu.position_id, None, f"emulated take profit {emu.tp}"))
            elif isinstance(emu, TimeExitEmu) and not emu.done and tick.ts >= emu.deadline:
                emu.done = True
                actions.append(ClosePosition(emu.position_id, None, "time exit"))
        return actions

    def _trail(
        self, emu: Trailing, tick: Tick, pos: Position, inst: Instrument | None, now_ms: int
    ) -> list[Action]:
        """Desired stop = price - distance (in the trade's direction), adopted only if it improves on the best
        known stop by at least ``step`` (and one tick). Sent at most once per ``min_modify_interval``."""
        price = _exit_price(emu.side, tick)
        if _profit(emu.side, emu.entry, price) < emu.activation:
            return []
        candidate = price - emu.distance * emu.side.sign
        if inst is not None:
            candidate = inst.round_price(candidate)
        step = max(emu.step, inst.tick_size if inst is not None else Decimal(0))
        best = emu.desired if emu.desired is not None else emu.stop if emu.stop is not None else pos.sl
        if best is None or (candidate - best) * emu.side.sign >= max(step, Decimal("1e-12")):
            emu.desired = candidate
        if emu.desired is None:
            return []
        interval_ms = self.min_modify_interval // timedelta(milliseconds=1)
        if emu.last_sent_ms is not None and now_ms - emu.last_sent_ms < interval_ms:
            return []  # throttled: the best stop goes out on a later tick
        stop, emu.stop, emu.desired, emu.last_sent_ms = emu.desired, emu.desired, None, now_ms
        return [ModifyStop(emu.position_id, stop, "trailing stop")]

    def _scale(self, emu: ScaleOutEmu, tick: Tick, pos: Position, inst: Instrument | None) -> list[Action]:
        actions: list[Action] = []
        profit = _profit(emu.side, emu.entry, _exit_price(emu.side, tick))
        while emu.next_level < len(emu.distances) and profit >= emu.distances[emu.next_level]:
            last = emu.next_level == len(emu.distances) - 1
            fraction = emu.fractions[emu.next_level]
            emu.next_level += 1
            remaining = pos.volume  # the live position already reflects earlier partial closes
            if remaining <= 0:
                break
            wanted = emu.initial_volume * fraction
            volume = floor_to_step(wanted, inst.volume_step) if inst else wanted
            close_all = last and sum(emu.fractions) >= 1
            if inst is not None and (remaining - volume < inst.min_volume):
                close_all = True
            if close_all or volume >= remaining:
                actions.append(
                    ClosePosition(emu.position_id, None, f"scale-out level {emu.next_level} (rest)")
                )
                emu.closed = pos.volume
                break
            if inst is not None and volume < inst.min_volume:
                continue  # level too small to trade: skip it
            emu.closed += volume
            actions.append(ClosePosition(emu.position_id, volume, f"scale-out level {emu.next_level}"))
        return actions

    def on_time(self, now: datetime) -> list[Action]:
        actions: list[Action] = []
        for emu in list(self.items.values()):
            if isinstance(emu, TimeExitEmu) and not emu.done and now >= emu.deadline:
                emu.done = True
                actions.append(ClosePosition(emu.position_id, None, "time exit"))
            elif isinstance(emu, ExpiryEmu) and not emu.done and now >= emu.deadline:
                emu.done = True
                actions.append(CancelOrder(emu.client_order_id, "expired (emulated)"))
        return actions
