"""The risk port the OMS calls (SPEC §2: Strategy -> intent -> RiskEngine.pre_trade -> OrderManager ->
Broker; nothing bypasses risk). ``trader.risk.engine.RiskEngine`` implements it.

Every method runs on the account's OMS writer task, so implementations are synchronous, fast and free of
IO. They never call the broker; they return a ``RiskOutput`` and the OMS acts on it (publish events,
cancel pending orders, flatten). Persistent state goes through ``dump``/``load`` inside the OMS snapshot.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from trader.domain.enums import KillLevel
from trader.domain.events import DomainEvent
from trader.domain.jsontypes import JsonObj
from trader.domain.models import AccountState, Instrument, Order, OrderIntent, OrderRequest, Position, Tick


@dataclass(frozen=True, slots=True, kw_only=True)
class AccountView:
    """What risk may look at: a read-only snapshot of the OMS's view of the account."""

    account_id: str
    account: AccountState | None
    positions: tuple[Position, ...]
    open_orders: tuple[Order, ...]
    instruments: Mapping[str, Instrument]
    last_ticks: Mapping[str, Tick]
    broker_connected: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class RiskVerdict:
    approved: bool
    rule: str
    reason: str
    volume: Decimal | None = None  # approved with a different (sized/capped) volume

    @classmethod
    def approve(cls, rule: str = "approved", reason: str = "", volume: Decimal | None = None) -> RiskVerdict:
        return cls(approved=True, rule=rule, reason=reason, volume=volume)

    @classmethod
    def reject(cls, rule: str, reason: str) -> RiskVerdict:
        return cls(approved=False, rule=rule, reason=reason)


@dataclass(frozen=True, slots=True)
class RiskOutput:
    """What the OMS must do after a risk hook. ``changed`` = persisted risk state changed."""

    events: tuple[DomainEvent, ...] = ()
    cancel_pending: bool = False
    flatten: bool = False
    changed: bool = False

    def __or__(self, other: RiskOutput) -> RiskOutput:
        return RiskOutput(
            events=self.events + other.events,
            cancel_pending=self.cancel_pending or other.cancel_pending,
            flatten=self.flatten or other.flatten,
            changed=self.changed or other.changed,
        )


NOTHING = RiskOutput()


class RiskGate:
    """The port, with no-op hooks; subclasses override what they need."""

    @property
    def level(self) -> KillLevel:
        return KillLevel.NONE

    def pre_trade(self, intent: OrderIntent, view: AccountView) -> RiskVerdict:
        """Approve or reject a new order."""
        raise NotImplementedError

    def pre_modify(self, current: OrderRequest, modified: OrderIntent, view: AccountView) -> RiskVerdict:
        """Approve or reject a change to a pending order (``modified`` = the order after the change)."""
        return RiskVerdict.approve(rule="modify", reason="no modification rules")

    def pre_modify_position(self, position: Position, sl: Decimal | None, view: AccountView) -> RiskVerdict:
        """Approve or reject a new stop for an open position (``None`` removes it)."""
        return RiskVerdict.approve(rule="modify_position", reason="no modification rules")

    def on_tick(self, tick: Tick) -> None:
        """Every tick, synchronously (keep it O(1))."""

    def observe(self, event: DomainEvent, view: AccountView) -> RiskOutput:
        """Every event the OMS publishes (fills, orders, account, reconcile, connection)."""
        return NOTHING

    def on_time(self, view: AccountView) -> RiskOutput:
        """The OMS timer (about once a second): time-based breakers and re-enforcement."""
        return NOTHING

    def kill(self, level: KillLevel, reason: str, view: AccountView) -> RiskOutput:
        """Manual kill switch (API/CLI). Latched until ``reset``."""
        raise NotImplementedError("this risk gate has no kill switch")

    def reset(self, reason: str, view: AccountView) -> RiskOutput:
        """Manual reset: clears the manual switch and latched breakers (live conditions re-trigger)."""
        raise NotImplementedError("this risk gate has no kill switch")

    def dump(self) -> JsonObj:
        return {}

    def load(self, data: JsonObj) -> None:
        """Restore what ``dump`` returned (after a restart)."""


class ApproveAll(RiskGate):
    """Approves everything. Only for tests; the composition root never uses it (it builds a RiskEngine)."""

    def pre_trade(self, intent: OrderIntent, view: AccountView) -> RiskVerdict:
        return RiskVerdict.approve(rule="approve_all", reason="no risk engine configured")
