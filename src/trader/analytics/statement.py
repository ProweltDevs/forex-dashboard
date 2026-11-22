"""``trader pnl-check``: our fill ledger vs the broker's statement for one trading day (phase 8
acceptance: "P&L matches broker statement for a demo day").

Checks, with tolerance 0.01 (account currency) per deal:
1. Every broker deal of the day is in our ledger and vice versa (by deal id).
2. Each matching deal has the same side, volume, price, gross, commission and swap.
3. The day's totals agree (gross, commission, swap, net).
4. Balance chain (where the broker reports the balance after a deal, e.g. cTrader closing deals): the change
   between two such deals equals the net of the deals in between.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from trader.domain.models import Fill
from trader.portfolio.ledger import PnL

from .types import OutObj

TOLERANCE = Decimal("0.01")
ZERO = Decimal(0)


def day_window(day: date, day_offset_minutes: int) -> tuple[datetime, datetime]:
    """[start, end) of a trading day in UTC (the day starts at 00:00 UTC + offset)."""
    start = datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(minutes=day_offset_minutes)
    return start, start + timedelta(days=1)


def _pnl(fills: Sequence[Fill]) -> PnL:
    out = PnL()
    for f in fills:
        out = out + PnL(gross=f.realized_pnl or ZERO, commission=f.commission, swap=f.swap, deals=1)
    return out


def _pnl_out(p: PnL) -> OutObj:
    return {
        "gross": str(p.gross),
        "commission": str(p.commission),
        "swap": str(p.swap),
        "net": str(p.net),
        "deals": p.deals,
    }


def _pnl_line(p: PnL) -> str:
    return f"{p.deals} deals, gross {p.gross}, commission {p.commission}, swap {p.swap}, net {p.net}"


def _fields(f: Fill) -> tuple[object, ...]:
    return (f.side, f.volume, f.price)


def _money(f: Fill) -> tuple[Decimal, Decimal, Decimal]:
    return (f.realized_pnl or ZERO, f.commission, f.swap)


@dataclass(slots=True)
class StatementCheck:
    day: date
    broker: PnL
    ours: PnL
    missing: list[Fill] = field(default_factory=list)  # broker deals we do not have
    extra: list[Fill] = field(default_factory=list)  # our deals the broker does not report
    mismatched: list[tuple[Fill, Fill]] = field(default_factory=list)  # (ours, broker)
    chain_breaks: list[str] = field(default_factory=list)
    chain_checked: int = 0

    @property
    def ok(self) -> bool:
        totals = all(
            abs(a - b) <= TOLERANCE * max(1, self.broker.deals)
            for a, b in (
                (self.broker.gross, self.ours.gross),
                (self.broker.commission, self.ours.commission),
                (self.broker.swap, self.ours.swap),
            )
        )
        return totals and not (self.missing or self.extra or self.mismatched or self.chain_breaks)

    def to_json(self) -> OutObj:
        return {
            "day": self.day.isoformat(),
            "ok": self.ok,
            "broker": _pnl_out(self.broker),
            "ours": _pnl_out(self.ours),
            "missing": [f.fill_id for f in self.missing],
            "extra": [f.fill_id for f in self.extra],
            "mismatched": [o.fill_id for o, _ in self.mismatched],
            "balance_chain_checked": self.chain_checked,
            "balance_chain_breaks": list(self.chain_breaks),
        }

    def lines(self) -> list[str]:
        b, o = self.broker, self.ours
        out = [
            f"pnl-check {self.day.isoformat()}: {'OK' if self.ok else 'MISMATCH'}",
            f"  broker: {_pnl_line(b)}",
            f"  ours:   {_pnl_line(o)}",
            f"  difference (ours - broker): net {o.net - b.net}",
            f"  balance chain: {self.chain_checked} links checked, {len(self.chain_breaks)} breaks",
        ]
        out += [
            f"  missing deal {f.fill_id} {f.symbol} {f.side} {f.volume} @ {f.price} net {f.net}"
            for f in self.missing
        ]
        out += [f"  extra deal {f.fill_id} {f.symbol} net {f.net}" for f in self.extra]
        out += [f"  differs {o.fill_id}: ours {_money(o)} broker {_money(t)}" for o, t in self.mismatched]
        out += [f"  {line}" for line in self.chain_breaks]
        return out


def compare(day: date, ours: Sequence[Fill], broker: Sequence[Fill]) -> StatementCheck:
    check = StatementCheck(day=day, broker=_pnl(broker), ours=_pnl(ours))
    mine = {f.fill_id: f for f in ours}
    theirs = {f.fill_id: f for f in broker}
    check.missing = [f for fid, f in theirs.items() if fid not in mine]
    check.extra = [f for fid, f in mine.items() if fid not in theirs]
    for fid, t in theirs.items():
        o = mine.get(fid)
        if o is None:
            continue
        if _fields(o) != _fields(t) or any(
            abs(a - b) > TOLERANCE for a, b in zip(_money(o), _money(t), strict=True)
        ):
            check.mismatched.append((o, t))
    ordered = sorted(broker, key=lambda f: (f.ts, f.fill_id))
    last_balance: Decimal | None = None
    running = ZERO
    for f in ordered:
        running += f.net
        if f.balance_after is None:
            continue
        if last_balance is not None:
            check.chain_checked += 1
            change = f.balance_after - last_balance
            if abs(change - running) > TOLERANCE * 2:
                check.chain_breaks.append(
                    f"balance after deal {f.fill_id} moved {change} but the deals in between net {running}"
                )
        last_balance, running = f.balance_after, ZERO
    return check
