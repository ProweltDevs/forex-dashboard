"""Request and response models of the Control API: the OpenAPI contract for the future UI (SPEC §13).
Money, prices and volumes are decimal strings (exact); times are ISO-8601 UTC."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from trader.domain.enums import KillLevel, OrderType, Side, TimeInForce


class _Out(BaseModel):
    model_config = ConfigDict(frozen=True)


class HealthOut(_Out):
    status: Literal["ok", "degraded"]
    accounts_connected: int
    accounts: int


class AccountOut(_Out):
    id: str
    broker: str
    environment: str
    connected: bool
    ready: bool
    kill_level: KillLevel


class AccountStateOut(_Out):
    id: str
    currency: str | None
    balance: Decimal | None
    equity: Decimal | None
    margin: Decimal | None
    free_margin: Decimal | None
    computed_equity: Decimal | None
    unrealized: Decimal | None
    realized_today: Decimal | None
    high_water: Decimal | None
    drawdown: Decimal | None
    positions: int
    open_orders: int
    kill_level: KillLevel
    risk_triggers: dict[str, str]


class PositionOut(_Out):
    account_id: str
    position_id: str
    symbol: str
    side: Side
    volume: Decimal
    entry_price: Decimal
    sl: Decimal | None
    tp: Decimal | None
    strategy_id: str
    opened_ts: datetime
    unrealized: Decimal | None


class OrderOut(_Out):
    account_id: str
    client_order_id: str
    broker_order_id: str | None
    strategy_id: str
    symbol: str
    side: Side
    order_type: OrderType
    volume: Decimal
    filled_volume: Decimal
    avg_fill_price: Decimal | None
    limit_price: Decimal | None
    stop_price: Decimal | None
    sl: Decimal | None
    tp: Decimal | None
    status: str
    rejection_reason: str | None
    created: datetime
    updated: datetime


class ManualOrderIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account: str
    symbol: str
    side: Side
    volume: Decimal = Field(gt=0)  # lots
    order_type: OrderType = OrderType.MARKET
    limit_price: Decimal | None = Field(default=None, gt=0)
    stop_price: Decimal | None = Field(default=None, gt=0)
    sl: Decimal | None = Field(default=None, gt=0)
    tp: Decimal | None = Field(default=None, gt=0)
    time_in_force: TimeInForce = TimeInForce.GTC
    expiry: datetime | None = None
    max_slippage: Decimal | None = Field(default=None, gt=0)  # price units
    reason: str = "manual order"


class ClosePositionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account: str
    volume: Decimal | None = Field(default=None, gt=0)  # None = the whole position
    reason: str = "manual close"


class CommandOut(_Out):
    ok: bool
    message: str


class StrategyOut(_Out):
    id: str
    account: str
    class_name: str
    version: str
    status: str
    pause_reason: str | None
    params: dict[str, Any]
    symbols: list[str]
    timeframes: list[str]
    callbacks: int
    intents: int
    errors: int
    slow_callbacks: int
    last_error: str | None


class StrategyCreateIn(BaseModel):
    """Same fields as a ``strategies:`` entry in the config."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    id: str
    class_: str = Field(alias="class")
    account: str
    symbols: list[str]
    timeframes: list[str] = []
    params: dict[str, Any] = {}
    max_errors: int = Field(default=5, ge=1)
    callback_budget_ms: float = Field(default=50.0, gt=0)
    seed: int = 0
    start: bool = True


class StrategyParamsIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    params: dict[str, Any]


class ReasonIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: str = "requested via API"


class RiskAccountOut(_Out):
    account_id: str
    level: KillLevel
    triggers: dict[str, str]
    day_start_equity: Decimal | None
    peak_equity: Decimal | None
    limits: dict[str, Any]
    breakers: dict[str, Any]


class KillIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account: str
    level: Literal[KillLevel.PAUSE_NEW, KillLevel.CANCEL_PENDING, KillLevel.FLATTEN_ALL]
    reason: str


class ResetIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account: str
    reason: str


class PnlLine(_Out):
    strategy_id: str
    symbol: str
    gross: Decimal
    commission: Decimal
    swap: Decimal
    net: Decimal
    deals: int


class PnlOut(_Out):
    account_id: str
    day: str
    gross: Decimal
    commission: Decimal
    swap: Decimal
    net: Decimal
    deals: int
    by_strategy_symbol: list[PnlLine]
    equity: Decimal | None
    unrealized: Decimal | None
    high_water: Decimal | None
    drawdown: Decimal | None


class TradeOut(_Out):
    account_id: str
    trade_id: str
    strategy_id: str
    strategy_version: str
    symbol: str
    side: Side
    volume: Decimal
    entry_price: Decimal
    exit_price: Decimal
    entry_time: datetime
    exit_time: datetime
    gross: Decimal
    commission: Decimal
    swap: Decimal
    net: Decimal
    r_multiple: Decimal | None
    mae: Decimal | None
    mfe: Decimal | None
    notes: list[str]


class NoteIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account: str
    text: str = Field(min_length=1, max_length=2000)


class StatsOut(_Out):
    key: dict[str, Any]
    stats: dict[str, Any]


class AuditOut(_Out):
    ts: datetime
    actor: str
    action: str
    account_id: str | None
    details: dict[str, Any]
    result: str
