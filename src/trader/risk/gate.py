"""The risk port the OMS calls for every new order (SPEC §2: Strategy -> intent -> RiskEngine.pre_trade ->
OrderManager -> Broker; nothing bypasses risk). Phase 6 implements the rules; this module defines the
contract and the account view the rules read."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from trader.domain.models import AccountState, Instrument, Order, OrderIntent, Position, Tick


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


class RiskGate(Protocol):
    def pre_trade(self, intent: OrderIntent, view: AccountView) -> RiskVerdict:
        """Approve or reject a new order. Must be fast and pure (runs on the OMS writer task)."""
        ...


class ApproveAll:
    """Approves everything. Only for tests and for running before the phase-6 RiskEngine exists; the
    composition root must never use it for live accounts."""

    def pre_trade(self, intent: OrderIntent, view: AccountView) -> RiskVerdict:
        return RiskVerdict.approve(rule="approve_all", reason="no risk engine configured")
