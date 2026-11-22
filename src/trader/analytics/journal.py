"""Trade journal (SPEC §10): one record per round trip, built from the fill ledger.

A trade is a broker position from its first fill to the fill that brings its volume back to zero:
- entry/exit prices are volume-weighted over the entry/exit fills; times are the first entry / last exit;
- gross, commission, swap and net are the sums over its deals (broker figures, account currency);
- MAE/MFE: the worst / best excursion from the entry while open, from ticks at the exit side (bid for
  longs, ask for shorts), in price and in money;
- R multiple: net / initial risk, the initial risk being the distance to the opening order's SL (none
  without an SL);
- SL/TP history from the order and every position update; strategy version and tags from the opening
  order; ``params_hash`` from the strategy config. Notes can be added later (``add_note``).

Storage: closed trades in ``{data_dir}/journal/{account}/trades.jsonl``, notes in ``notes.jsonl``.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from trader.domain.enums import Side
from trader.domain.errors import ConversionUnavailable
from trader.domain.events import PositionEvent
from trader.domain.fx import conversion_rate
from trader.domain.jsontypes import JsonObj, JsonValue
from trader.domain.models import Fill, Instrument, Order, Tick
from trader.domain.time import from_epoch_ms, to_epoch_ms
from trader.observability.logging import get_logger
from trader.portfolio.ledger import LedgerEntry
from trader.portfolio.service import Listener

_log = get_logger("trader.journal")
ZERO = Decimal(0)


def _vwap(fills: list[Fill]) -> Decimal:
    volume = sum((f.volume for f in fills), ZERO)
    return sum((f.price * f.volume for f in fills), ZERO) / volume


@dataclass(slots=True)
class _Open:
    trade_id: str
    strategy_id: str
    symbol: str
    side: Side
    entries: list[Fill] = field(default_factory=list)
    exits: list[Fill] = field(default_factory=list)
    volume: Decimal = ZERO  # currently open
    max_volume: Decimal = ZERO
    sl_history: list[tuple[datetime, Decimal | None, Decimal | None]] = field(default_factory=list)
    initial_sl: Decimal | None = None
    version: str = ""
    tags: tuple[str, ...] = ()
    mae: Decimal = ZERO  # adverse excursion, price units, >= 0
    mfe: Decimal = ZERO  # favourable excursion, price units, >= 0


@dataclass(frozen=True, slots=True, kw_only=True)
class TradeRecord:
    trade_id: str
    account_id: str
    strategy_id: str
    strategy_version: str
    params_hash: str
    symbol: str
    side: Side
    volume: Decimal  # largest open volume
    entry_price: Decimal
    exit_price: Decimal
    entry_time: datetime
    exit_time: datetime
    gross: Decimal
    commission: Decimal
    swap: Decimal
    initial_sl: Decimal | None
    sl_history: tuple[tuple[datetime, Decimal | None, Decimal | None], ...]
    mae_price: Decimal
    mfe_price: Decimal
    mae: Decimal | None  # money (None without a conversion rate at close)
    mfe: Decimal | None
    risk: Decimal | None  # money at risk at entry (needs an SL)
    tags: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    @property
    def net(self) -> Decimal:
        return self.gross + self.commission + self.swap

    @property
    def r_multiple(self) -> Decimal | None:
        return None if not self.risk else self.net / self.risk

    @property
    def duration_s(self) -> float:
        return (self.exit_time - self.entry_time).total_seconds()

    def to_json(self) -> JsonObj:
        def s(v: Decimal | None) -> str | None:
            return None if v is None else str(v)

        history: list[JsonValue] = [[to_epoch_ms(ts), s(sl), s(tp)] for ts, sl, tp in self.sl_history]
        r = self.r_multiple
        return {
            "trade_id": self.trade_id,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "params_hash": self.params_hash,
            "symbol": self.symbol,
            "side": self.side.value,
            "volume": str(self.volume),
            "entry_price": str(self.entry_price),
            "exit_price": str(self.exit_price),
            "entry_time_ms": to_epoch_ms(self.entry_time),
            "exit_time_ms": to_epoch_ms(self.exit_time),
            "gross": str(self.gross),
            "commission": str(self.commission),
            "swap": str(self.swap),
            "net": str(self.net),
            "initial_sl": s(self.initial_sl),
            "sl_history": history,
            "mae_price": str(self.mae_price),
            "mfe_price": str(self.mfe_price),
            "mae": s(self.mae),
            "mfe": s(self.mfe),
            "risk": s(self.risk),
            "r_multiple": s(None if r is None else r.quantize(Decimal("0.0001"))),
            "tags": list(self.tags),
        }

    @classmethod
    def from_json(cls, d: JsonObj, notes: tuple[str, ...] = ()) -> TradeRecord:
        def od(v: JsonValue) -> Decimal | None:
            return None if v is None else Decimal(str(v))

        raw_hist = d.get("sl_history") or []
        assert isinstance(raw_hist, list)
        history = tuple(
            (from_epoch_ms(int(str(h[0]))), od(h[1]), od(h[2])) for h in raw_hist if isinstance(h, list)
        )
        tags = d.get("tags") or []
        assert isinstance(tags, list)
        return cls(
            trade_id=str(d["trade_id"]),
            account_id=str(d["account_id"]),
            strategy_id=str(d["strategy_id"]),
            strategy_version=str(d.get("strategy_version") or ""),
            params_hash=str(d.get("params_hash") or ""),
            symbol=str(d["symbol"]),
            side=Side(str(d["side"])),
            volume=Decimal(str(d["volume"])),
            entry_price=Decimal(str(d["entry_price"])),
            exit_price=Decimal(str(d["exit_price"])),
            entry_time=from_epoch_ms(int(str(d["entry_time_ms"]))),
            exit_time=from_epoch_ms(int(str(d["exit_time_ms"]))),
            gross=Decimal(str(d["gross"])),
            commission=Decimal(str(d["commission"])),
            swap=Decimal(str(d["swap"])),
            initial_sl=od(d.get("initial_sl")),
            sl_history=history,
            mae_price=Decimal(str(d["mae_price"])),
            mfe_price=Decimal(str(d["mfe_price"])),
            mae=od(d.get("mae")),
            mfe=od(d.get("mfe")),
            risk=od(d.get("risk")),
            tags=tuple(str(t) for t in tags),
            notes=notes,
        )


class TradeJournal(Listener):
    def __init__(
        self,
        account_id: str,
        *,
        currency: Callable[[], str | None],
        instruments: Callable[[], Mapping[str, Instrument]],
        order_lookup: Callable[[str], Order | None],
        params_hash: Mapping[str, str] | None = None,
        directory: Path | None = None,
    ) -> None:
        self.account_id = account_id
        self._currency, self._instruments, self._order = currency, instruments, order_lookup
        self._params_hash = dict(params_hash or {})
        self.dir = directory
        self.open: dict[str, _Open] = {}
        self.closed: list[TradeRecord] = []
        self._closed_ids: set[str] = set()
        self.on_closed: list[Callable[[TradeRecord], None]] = []  # e.g. the database writer
        self._ticks: dict[str, Tick] = {}
        self._notes: dict[str, list[str]] = {}
        self._pending: list[str] = []
        self._pending_notes: list[str] = []

    # ------------------------------------------------------------------ listener

    def on_entry(self, entry: LedgerEntry) -> None:
        f = entry.fill
        trade = self.open.get(f.position_id)
        if trade is None:
            trade = self.open[f.position_id] = self._new(entry)
        if f.side is trade.side:
            trade.entries.append(f)
            trade.volume += f.volume
            trade.max_volume = max(trade.max_volume, trade.volume)
            return
        trade.exits.append(f)
        trade.volume -= f.volume
        if trade.volume <= 0:
            del self.open[f.position_id]
            if trade.entries:
                self._close(trade)
            else:
                _log.info("journal_exit_without_entry", account=self.account_id, position=f.position_id)

    def _new(self, entry: LedgerEntry) -> _Open:
        f = entry.fill
        trade = _Open(trade_id=f.position_id, strategy_id=entry.strategy_id, symbol=f.symbol, side=f.side)
        order = self._order(f.client_order_id) if f.client_order_id else None
        if order is not None:
            req = order.request
            trade.initial_sl, trade.version, trade.tags = req.sl, req.strategy_version, req.tags
            trade.sl_history.append((f.ts, req.sl, req.tp))
        return trade

    def on_replay(self, entry: LedgerEntry) -> None:
        """Startup: rebuild open trades from the ledger; trades already in trades.jsonl stay closed."""
        if not self.known(entry.fill.position_id):
            self.on_entry(entry)

    def on_position(self, event: PositionEvent) -> None:
        trade = self.open.get(event.position.position_id)
        if trade is None:
            return
        pos = event.position
        last = trade.sl_history[-1][1:] if trade.sl_history else None
        if last != (pos.sl, pos.tp):
            trade.sl_history.append((event.ts, pos.sl, pos.tp))
            if trade.initial_sl is None and pos.sl is not None and not trade.exits:
                trade.initial_sl = pos.sl

    def on_tick(self, tick: Tick) -> None:
        self._ticks[tick.symbol] = tick
        for trade in self.open.values():
            if trade.symbol != tick.symbol or not trade.entries:
                continue
            entry = _vwap(trade.entries)
            move = (tick.bid - entry) if trade.side is Side.BUY else (entry - tick.ask)
            trade.mfe = max(trade.mfe, move)
            trade.mae = max(trade.mae, -move)

    def flush(self) -> None:
        if self.dir is None:
            return
        trades, self._pending = self._pending, []
        notes, self._pending_notes = self._pending_notes, []
        self.dir.mkdir(parents=True, exist_ok=True)
        for name, lines in (("trades.jsonl", trades), ("notes.jsonl", notes)):
            if lines:
                with (self.dir / name).open("a", encoding="utf-8") as fh:
                    fh.write("\n".join(lines) + "\n")

    # ------------------------------------------------------------------ records

    def _close(self, t: _Open) -> None:
        deals = t.entries + t.exits
        inst = self._instruments().get(t.symbol)
        rate = self._rate(inst)
        per_price = None if inst is None or rate is None else t.max_volume * inst.contract_size * rate
        entry = _vwap(t.entries)
        risk = None if t.initial_sl is None or per_price is None else abs(entry - t.initial_sl) * per_price
        record = TradeRecord(
            trade_id=t.trade_id,
            account_id=self.account_id,
            strategy_id=t.strategy_id,
            strategy_version=t.version,
            params_hash=self._params_hash.get(t.strategy_id, ""),
            symbol=t.symbol,
            side=t.side,
            volume=t.max_volume,
            entry_price=entry,
            exit_price=_vwap(t.exits),
            entry_time=t.entries[0].ts,
            exit_time=t.exits[-1].ts,
            gross=sum((f.realized_pnl or ZERO for f in deals), ZERO),
            commission=sum((f.commission for f in deals), ZERO),
            swap=sum((f.swap for f in deals), ZERO),
            initial_sl=t.initial_sl,
            sl_history=tuple(t.sl_history),
            mae_price=t.mae,
            mfe_price=t.mfe,
            mae=None if per_price is None else t.mae * per_price,
            mfe=None if per_price is None else t.mfe * per_price,
            risk=risk,
            tags=t.tags,
        )
        self.closed.append(record)
        self._closed_ids.add(record.trade_id)
        self._pending.append(json.dumps(record.to_json(), separators=(",", ":")))
        for callback in self.on_closed:
            callback(record)

    def _rate(self, inst: Instrument | None) -> Decimal | None:
        ccy = self._currency()
        if inst is None or ccy is None:
            return None
        try:
            return conversion_rate(inst.quote_currency, ccy, self._ticks.get)
        except ConversionUnavailable:
            return None

    def add_note(self, trade_id: str, text: str) -> None:
        self._notes.setdefault(trade_id, []).append(text)
        self._pending_notes.append(json.dumps({"trade_id": trade_id, "note": text}, separators=(",", ":")))

    def load(self) -> int:
        """Closed trades from disk (blocking). Open trades are rebuilt by replaying the ledger."""
        if self.dir is None:
            return 0
        notes_path = self.dir / "notes.jsonl"
        if notes_path.exists():
            for line in notes_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    n = json.loads(line)
                    self._notes.setdefault(str(n["trade_id"]), []).append(str(n["note"]))
        path = self.dir / "trades.jsonl"
        if not path.exists():
            return 0
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                raw = json.loads(line)
                record = TradeRecord.from_json(raw, tuple(self._notes.get(str(raw["trade_id"]), [])))
                self.closed.append(record)
                self._closed_ids.add(record.trade_id)
        return len(self.closed)

    def known(self, trade_id: str) -> bool:
        return trade_id in self._closed_ids

    def trades(self) -> list[TradeRecord]:
        return [
            t if not self._notes.get(t.trade_id) else _with_notes(t, tuple(self._notes[t.trade_id]))
            for t in self.closed
        ]


def _with_notes(t: TradeRecord, notes: tuple[str, ...]) -> TradeRecord:
    return replace(t, notes=notes)
