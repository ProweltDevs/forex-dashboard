"""Performance metrics over closed trades (SPEC §10). Pure functions; money stays Decimal.

Definitions (ADR 0010):
- win: net > 0, loss: net < 0 (net = after commission and swap); win rate = wins / trades.
- profit factor = sum of winning nets / |sum of losing nets| (None without losses).
- expectancy = average net per trade; avg R over trades with an initial risk (an SL).
- drawdown: on the equity curve built from trade nets in exit order, starting at ``start_equity`` (0 if
  unknown, then no %); recovery factor = net / max drawdown.
- Sharpe / Sortino: daily returns (net of trades exiting that day / equity at the day's start) over every
  weekday in the period (no trades = 0 %), annualised with sqrt(252); None for fewer than two days.
- exposure: share of the period with at least one trade open; trades per day over weekdays.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

from .journal import TradeRecord
from .types import OutObj

ZERO = Decimal(0)
TRADING_DAYS = 252


@dataclass(frozen=True, slots=True, kw_only=True)
class Metrics:
    trades: int
    wins: int
    losses: int
    net: Decimal
    gross: Decimal
    costs: Decimal
    avg_win: Decimal | None
    avg_loss: Decimal | None
    profit_factor: Decimal | None
    expectancy: Decimal | None
    avg_r: Decimal | None
    max_drawdown: Decimal
    max_drawdown_pct: Decimal | None
    recovery_factor: Decimal | None
    sharpe: float | None
    sortino: float | None
    longest_win_streak: int
    longest_loss_streak: int
    exposure: float | None  # 0..1
    trades_per_day: float | None

    @property
    def win_rate(self) -> float | None:
        return None if not self.trades else self.wins / self.trades

    def to_json(self) -> OutObj:
        def s(v: Decimal | None, q: str = "0.01") -> str | None:
            return None if v is None else str(v.quantize(Decimal(q)))

        def f(v: float | None) -> float | None:
            return None if v is None else round(v, 4)

        return {
            "trades": self.trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": f(self.win_rate),
            "net": s(self.net),
            "gross": s(self.gross),
            "costs": s(self.costs),
            "avg_win": s(self.avg_win),
            "avg_loss": s(self.avg_loss),
            "profit_factor": s(self.profit_factor, "0.0001"),
            "expectancy": s(self.expectancy),
            "avg_r": s(self.avg_r, "0.0001"),
            "max_drawdown": s(self.max_drawdown),
            "max_drawdown_pct": s(self.max_drawdown_pct, "0.0001"),
            "recovery_factor": s(self.recovery_factor, "0.0001"),
            "sharpe": f(self.sharpe),
            "sortino": f(self.sortino),
            "longest_win_streak": self.longest_win_streak,
            "longest_loss_streak": self.longest_loss_streak,
            "exposure": f(self.exposure),
            "trades_per_day": f(self.trades_per_day),
        }


def _weekdays(start: date, end: date) -> list[date]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def _streaks(nets: Sequence[Decimal]) -> tuple[int, int]:
    best_win = best_loss = win = loss = 0
    for n in nets:
        win, loss = (win + 1, 0) if n > 0 else (0, loss + 1) if n < 0 else (0, 0)
        best_win, best_loss = max(best_win, win), max(best_loss, loss)
    return best_win, best_loss


def _exposure(trades: Sequence[TradeRecord], start: datetime, end: datetime) -> float | None:
    span = (end - start).total_seconds()
    if span <= 0:
        return None
    covered, cursor = 0.0, start
    for t in sorted(trades, key=lambda t: t.entry_time):
        a, b = max(t.entry_time, cursor), min(t.exit_time, end)
        if b > a:
            covered += (b - a).total_seconds()
            cursor = b
    return covered / span


def _ratios(returns: Sequence[float]) -> tuple[float | None, float | None]:
    if len(returns) < 2:
        return None, None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    down = math.sqrt(sum(min(r, 0.0) ** 2 for r in returns) / len(returns))
    scale = math.sqrt(TRADING_DAYS)
    sharpe = None if var == 0 else mean / math.sqrt(var) * scale
    sortino = None if down == 0 else mean / down * scale
    return sharpe, sortino


def compute(
    trades: Iterable[TradeRecord],
    *,
    start_equity: Decimal | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Metrics:
    ts = sorted(trades, key=lambda t: t.exit_time)
    nets = [t.net for t in ts]
    wins = [n for n in nets if n > 0]
    losses = [n for n in nets if n < 0]
    net = sum(nets, ZERO)
    rs = [r for t in ts if (r := t.r_multiple) is not None]

    equity = peak = start_equity or ZERO
    max_dd = ZERO
    max_dd_pct: Decimal | None = ZERO if start_equity else None
    for n in nets:
        equity += n
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)
        if max_dd_pct is not None and peak > 0:
            max_dd_pct = max(max_dd_pct, dd / peak * 100)

    sharpe = sortino = exposure = per_day = None
    if ts:
        first = start or min(t.entry_time for t in ts)
        last = end or max(t.exit_time for t in ts)
        days = _weekdays(first.date(), last.date())
        per_day = len(ts) / max(1, len(days))
        exposure = _exposure(ts, first, last)
        if start_equity:
            by_day: dict[date, Decimal] = {}
            for t in ts:
                by_day[t.exit_time.date()] = by_day.get(t.exit_time.date(), ZERO) + t.net
            running, returns = start_equity, []
            for d in days:
                pnl = by_day.get(d, ZERO)
                returns.append(float(pnl / running) if running > 0 else 0.0)
                running += pnl
            sharpe, sortino = _ratios(returns)
    win_streak, loss_streak = _streaks(nets)
    return Metrics(
        trades=len(ts),
        wins=len(wins),
        losses=len(losses),
        net=net,
        gross=sum((t.gross for t in ts), ZERO),
        costs=sum((t.commission + t.swap for t in ts), ZERO),
        avg_win=sum(wins, ZERO) / len(wins) if wins else None,
        avg_loss=sum(losses, ZERO) / len(losses) if losses else None,
        profit_factor=sum(wins, ZERO) / -sum(losses, ZERO) if losses else None,
        expectancy=net / len(ts) if ts else None,
        avg_r=sum(rs, ZERO) / len(rs) if rs else None,
        max_drawdown=max_dd,
        max_drawdown_pct=max_dd_pct,
        recovery_factor=net / max_dd if max_dd > 0 else None,
        sharpe=sharpe,
        sortino=sortino,
        longest_win_streak=win_streak,
        longest_loss_streak=loss_streak,
        exposure=exposure,
        trades_per_day=per_day,
    )


def group[K](
    trades: Iterable[TradeRecord], key: Callable[[TradeRecord], K], **kw: Decimal | datetime | None
) -> dict[K, Metrics]:
    buckets: dict[K, list[TradeRecord]] = {}
    for t in trades:
        buckets.setdefault(key(t), []).append(t)
    return {k: compute(v, **kw) for k, v in buckets.items()}  # type: ignore[arg-type]
