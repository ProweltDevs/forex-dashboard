"""Which events become notices (SPEC §14): fills, SL/TP hits, rejects, risk breaches and the kill switch,
disconnect/reconnect, stale feeds, reconcile corrections, operator alerts, strategy errors.

SL/TP hits are recognised from prices: a closing fill at (or beyond, within 10% of the stop distance short
of) the position's last known SL is reported as "SL hit", likewise for TP. Brokers do not tell every
adapter why a position closed; the notice says "closed at SL" because that is what the prices show.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal

from trader.domain.enums import ConnectionState, FeedState, KillLevel, OrderStatus, PositionChange, Side
from trader.domain.events import (
    AlertEvent,
    ConnectionEvent,
    FeedEvent,
    FillEvent,
    OrderEvent,
    PositionEvent,
    ReconcileEvent,
    RiskStateEvent,
    StrategyEvent,
)
from trader.domain.models import Fill, Order, Position

from .notifier import Notice, Severity

_TOLERANCE = Decimal("0.1")

type OrderLookup = Callable[[str, str], Order | None]  # (account_id, client_order_id)


def classify_exit(pos: Position, price: Decimal) -> str | None:
    """ "SL", "TP" or None for a closing fill at ``price``."""
    direction = 1 if pos.side is Side.BUY else -1
    if pos.sl is not None:
        tol = abs(pos.entry_price - pos.sl) * _TOLERANCE
        if (price - pos.sl) * direction <= tol:
            return "SL"
    if pos.tp is not None:
        tol = abs(pos.tp - pos.entry_price) * _TOLERANCE
        if (pos.tp - price) * direction <= tol:
            return "TP"
    return None


@dataclass(slots=True)
class EventRules:
    fills: bool = True  # notify every fill; SL/TP hits and closes are always notified
    orders: OrderLookup | None = None
    _positions: dict[tuple[str, str], Position] = field(default_factory=dict)
    _connection: dict[str, ConnectionState] = field(default_factory=dict)
    _stale: set[tuple[str, str]] = field(default_factory=set)

    def notice(self, event: object) -> Notice | None:
        match event:
            case FillEvent():
                return self._fill(event)
            case PositionEvent():
                key = (event.account_id, event.position.position_id)
                if event.change is PositionChange.CLOSED:
                    self._positions.pop(key, None)
                else:
                    self._positions[key] = event.position
                return None
            case OrderEvent():
                return self._order(event)
            case RiskStateEvent():
                return self._risk(event)
            case ConnectionEvent():
                return self._conn(event)
            case FeedEvent():
                return self._feed(event)
            case ReconcileEvent():
                return Notice(
                    key=f"reconcile:{event.account_id}:{event.kind}:{event.entity_id}",
                    severity=Severity.WARNING,
                    title=f"Reconcile: {event.kind}",
                    text=f"{event.account_id} {event.entity_id}: {event.before or '-'} -> "
                    f"{event.after or '-'} ({event.reason}). The broker's state was applied.",
                    ts=event.ts,
                    account_id=event.account_id,
                )
            case AlertEvent():
                sev = {"critical": Severity.CRITICAL, "warning": Severity.WARNING}.get(
                    event.severity, Severity.INFO
                )
                return Notice(
                    key=f"alert:{event.account_id}:{event.kind}",
                    severity=sev,
                    title=event.kind.replace("_", " "),
                    text=f"{event.account_id}: {event.message}" if event.account_id != "*" else event.message,
                    ts=event.ts,
                    account_id=None if event.account_id == "*" else event.account_id,
                )
            case StrategyEvent():
                return self._strategy(event)
        return None

    def _strategy_of(self, account_id: str, fill: Fill) -> str:
        if fill.client_order_id and self.orders is not None:
            order = self.orders(account_id, fill.client_order_id)
            if order is not None:
                return order.request.strategy_id
        pos = self._positions.get((account_id, fill.position_id))
        return pos.strategy_id if pos is not None else "?"

    def _fill(self, event: FillEvent) -> Notice | None:
        f, acct = event.fill, event.account_id
        pos = self._positions.get((acct, f.position_id))
        strategy = self._strategy_of(acct, f)
        closing = pos is not None and f.side is pos.side.opposite
        if closing:
            assert pos is not None
            hit = classify_exit(pos, f.price)
            pnl = "" if f.realized_pnl is None else f", P&L {f.realized_pnl + f.commission + f.swap:.2f}"
            title = f"{hit} hit" if hit else "Position closed"
            return Notice(
                key=f"fill:{f.fill_id}",
                severity=Severity.INFO,
                title=f"{title} {f.symbol}",
                text=f"{acct} {strategy}: {pos.side} {f.volume} {f.symbol} closed @ {f.price} "
                f"(entry {pos.entry_price}, SL {pos.sl or '-'}, TP {pos.tp or '-'}){pnl}",
                ts=event.ts,
                account_id=acct,
            )
        if not self.fills:
            return None
        return Notice(
            key=f"fill:{f.fill_id}",
            severity=Severity.INFO,
            title=f"Fill {f.side} {f.symbol}",
            text=f"{acct} {strategy}: {f.side} {f.volume} {f.symbol} @ {f.price}",
            ts=event.ts,
            account_id=acct,
        )

    def _order(self, event: OrderEvent) -> Notice | None:
        o = event.order
        if o.status is not OrderStatus.REJECTED or event.previous_status is OrderStatus.REJECTED:
            return None
        r = o.request
        reason = o.rejection_reason or "rejected"
        rule = ":".join(reason.split(":")[:2]) if reason.startswith("risk:") else reason.split(":")[0]
        return Notice(
            key=f"reject:{event.account_id}:{r.strategy_id}:{rule}",
            severity=Severity.WARNING,
            title=f"Order rejected {r.symbol}",
            text=f"{event.account_id} {r.strategy_id}: {r.side} {r.volume} {r.symbol} {r.order_type}: "
            f"{reason}",
            ts=event.ts,
            account_id=event.account_id,
        )

    def _risk(self, event: RiskStateEvent) -> Notice | None:
        if event.level == event.previous:
            return None
        if event.level is KillLevel.NONE:
            return Notice(
                key=f"risk:{event.account_id}:NONE",
                severity=Severity.INFO,
                title="Risk cleared",
                text=f"{event.account_id}: kill switch off ({event.reason}); strategies resume",
                ts=event.ts,
                account_id=event.account_id,
            )
        up = event.level.rank > event.previous.rank
        return Notice(
            key=f"risk:{event.account_id}:{event.level}",
            severity=Severity.CRITICAL if up else Severity.WARNING,
            title=f"Kill switch {event.level}",
            text=f"{event.account_id}: {event.previous} -> {event.level}: {event.reason} "
            f"(active: {', '.join(event.triggers) or '-'})",
            ts=event.ts,
            account_id=event.account_id,
        )

    def _conn(self, event: ConnectionEvent) -> Notice | None:
        before = self._connection.get(event.account_id)
        self._connection[event.account_id] = event.state
        if event.state is ConnectionState.UP:
            if before is None or before is ConnectionState.UP:
                return None  # the first connect is in the startup notice
            return Notice(
                key=f"conn:{event.account_id}:UP",
                severity=Severity.INFO,
                title="Reconnected",
                text=f"{event.account_id} ({event.broker}) is connected again; reconciling before trading",
                ts=event.ts,
                account_id=event.account_id,
            )
        return Notice(
            key=f"conn:{event.account_id}:{event.state}",
            severity=Severity.WARNING,
            title="Disconnected" if event.state is ConnectionState.DOWN else "Connection degraded",
            text=f"{event.account_id} ({event.broker}): {event.state}"
            + (f" ({event.reason})" if event.reason else ""),
            ts=event.ts,
            account_id=event.account_id,
        )

    def _feed(self, event: FeedEvent) -> Notice | None:
        key = (event.broker, event.symbol)
        if event.state is FeedState.STALE:
            self._stale.add(key)
            return Notice(
                key=f"feed:{event.broker}:{event.symbol}:STALE",
                severity=Severity.WARNING,
                title=f"Stale feed {event.symbol}",
                text=f"{event.broker} {event.symbol}: no ticks for {event.silence_ms / 1000:.0f} s",
                ts=event.ts,
            )
        if key not in self._stale:
            return None
        self._stale.discard(key)
        return Notice(
            key=f"feed:{event.broker}:{event.symbol}:LIVE",
            severity=Severity.INFO,
            title=f"Feed back {event.symbol}",
            text=f"{event.broker} {event.symbol}: ticks again",
            ts=event.ts,
        )

    @staticmethod
    def _strategy(event: StrategyEvent) -> Notice | None:
        if event.kind == "error":
            return Notice(
                key=f"strategy_error:{event.strategy_id}",
                severity=Severity.WARNING,
                title=f"Strategy error {event.strategy_id}",
                text=event.message,
                ts=event.ts,
            )
        if event.kind == "paused" and not event.message.startswith(("kill switch", "shutdown")):
            return Notice(
                key=f"strategy_paused:{event.strategy_id}",
                severity=Severity.WARNING,
                title=f"Strategy paused {event.strategy_id}",
                text=event.message,
                ts=event.ts,
            )
        return None
