"""Fill ledger: every broker deal of an account, attributed to a strategy and a trading day (SPEC §9).

Realized P&L comes only from here, i.e. from broker deals: ``gross`` (the deal's realized profit),
``commission`` and ``swap`` as the broker reported them; ``net`` is what the deal did to the balance. The
ledger is filled from live fills and backfilled from ``Broker.deal_history`` (missed fills, e.g. during a
disconnect), and it is deduplicated by deal id, so the order of arrival does not matter.

Storage: one JSON line per deal in ``{data_dir}/portfolio/{account}/fills.jsonl`` (append-only).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from trader.domain.enums import Side
from trader.domain.jsontypes import JsonObj
from trader.domain.models import Fill
from trader.domain.time import from_epoch_ms, to_epoch_ms

ZERO = Decimal(0)


@dataclass(frozen=True, slots=True)
class PnL:
    gross: Decimal = ZERO
    commission: Decimal = ZERO
    swap: Decimal = ZERO
    deals: int = 0

    @property
    def costs(self) -> Decimal:
        return self.commission + self.swap

    @property
    def net(self) -> Decimal:
        return self.gross + self.commission + self.swap

    def __add__(self, other: PnL) -> PnL:
        return PnL(
            gross=self.gross + other.gross,
            commission=self.commission + other.commission,
            swap=self.swap + other.swap,
            deals=self.deals + other.deals,
        )

    def to_json(self) -> JsonObj:
        return {
            "gross": str(self.gross),
            "commission": str(self.commission),
            "swap": str(self.swap),
            "net": str(self.net),
            "deals": self.deals,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class LedgerEntry:
    fill: Fill
    strategy_id: str  # "manual" when the position is not ours
    day: date  # trading day (risk ``day_offset_minutes``)

    @property
    def pnl(self) -> PnL:
        f = self.fill
        return PnL(gross=f.realized_pnl or ZERO, commission=f.commission, swap=f.swap, deals=1)


def trading_day(ts: datetime, day_offset_minutes: int) -> date:
    return (ts - timedelta(minutes=day_offset_minutes)).date()


def fill_to_json(f: Fill) -> JsonObj:
    def opt(v: Decimal | None) -> str | None:
        return None if v is None else str(v)

    return {
        "fill_id": f.fill_id,
        "client_order_id": f.client_order_id,
        "broker_order_id": f.broker_order_id,
        "position_id": f.position_id,
        "symbol": f.symbol,
        "side": f.side.value,
        "volume": str(f.volume),
        "price": str(f.price),
        "ts_ms": to_epoch_ms(f.ts),
        "commission": str(f.commission),
        "swap": str(f.swap),
        "realized_pnl": opt(f.realized_pnl),
        "balance_after": opt(f.balance_after),
    }


def fill_from_json(d: JsonObj) -> Fill:
    def opt(v: object) -> Decimal | None:
        return None if v is None else Decimal(str(v))

    cid = d.get("client_order_id")
    return Fill(
        fill_id=str(d["fill_id"]),
        client_order_id=None if cid is None else str(cid),
        broker_order_id=str(d["broker_order_id"]),
        position_id=str(d["position_id"]),
        symbol=str(d["symbol"]),
        side=Side(str(d["side"])),
        volume=Decimal(str(d["volume"])),
        price=Decimal(str(d["price"])),
        ts=from_epoch_ms(int(str(d["ts_ms"]))),
        commission=Decimal(str(d["commission"])),
        swap=Decimal(str(d["swap"])),
        realized_pnl=opt(d.get("realized_pnl")),
        balance_after=opt(d.get("balance_after")),
    )


class FillLedger:
    def __init__(self, path: Path | None, day_offset_minutes: int = 0) -> None:
        self.path = path
        self.day_offset = day_offset_minutes
        self._entries: list[LedgerEntry] = []
        self._ids: set[str] = set()
        self._pending: list[str] = []  # JSON lines not yet written

    # ------------------------------------------------------------------ input

    def add(self, fill: Fill, strategy_id: str) -> LedgerEntry | None:
        """Record a deal once (by deal id). Returns the entry if it was new."""
        if fill.fill_id in self._ids:
            return None
        entry = LedgerEntry(fill=fill, strategy_id=strategy_id, day=trading_day(fill.ts, self.day_offset))
        self._ids.add(fill.fill_id)
        self._entries.append(entry)
        self._pending.append(
            json.dumps({**fill_to_json(fill), "strategy_id": strategy_id}, separators=(",", ":"))
        )
        return entry

    def __contains__(self, fill_id: object) -> bool:
        return fill_id in self._ids

    def __len__(self) -> int:
        return len(self._entries)

    # ------------------------------------------------------------------ storage (blocking: call off-loop)

    def load(self) -> int:
        if self.path is None or not self.path.exists():
            return 0
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            raw = json.loads(line)
            fill = fill_from_json(raw)
            if fill.fill_id not in self._ids:
                self._ids.add(fill.fill_id)
                self._entries.append(
                    LedgerEntry(
                        fill=fill,
                        strategy_id=str(raw.get("strategy_id") or "manual"),
                        day=trading_day(fill.ts, self.day_offset),
                    )
                )
        return len(self._entries)

    @property
    def has_pending(self) -> bool:
        return bool(self._pending)

    def take_pending(self) -> list[str]:
        lines, self._pending = self._pending, []
        return lines

    def append_lines(self, lines: list[str]) -> None:
        if self.path is None or not lines:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
            fh.flush()

    # ------------------------------------------------------------------ queries

    def entries(self, *, day: date | None = None, since: datetime | None = None) -> Iterator[LedgerEntry]:
        for e in self._entries:
            if (day is None or e.day == day) and (since is None or e.fill.ts > since):
                yield e

    def last_ts(self) -> datetime | None:
        return max((e.fill.ts for e in self._entries), default=None)

    def realized(
        self, *, day: date | None = None, strategy_id: str | None = None, symbol: str | None = None
    ) -> PnL:
        return total(
            e
            for e in self.entries(day=day)
            if (strategy_id is None or e.strategy_id == strategy_id)
            and (symbol is None or e.fill.symbol == symbol)
        )

    def attribution(self, day: date | None = None) -> dict[tuple[str, str], PnL]:
        """Realized P&L per (strategy, symbol)."""
        out: dict[tuple[str, str], PnL] = {}
        for e in self.entries(day=day):
            key = (e.strategy_id, e.fill.symbol)
            out[key] = out.get(key, PnL()) + e.pnl
        return out

    def days(self) -> list[date]:
        return sorted({e.day for e in self._entries})


def total(entries: Iterable[LedgerEntry]) -> PnL:
    out = PnL()
    for e in entries:
        out = out + e.pnl
    return out
