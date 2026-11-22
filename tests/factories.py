"""Small builders for domain objects used across tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from trader.domain import Instrument, Order, OrderIntent, OrderRequest, OrderType, Side
from trader.domain.ids import new_client_order_id

T0 = datetime(2026, 1, 5, 8, 0, tzinfo=UTC)


def eurusd(**overrides: object) -> Instrument:
    fields: dict[str, object] = {
        "symbol": "EURUSD",
        "broker_symbol": "EURUSD",
        "broker_id": "1",
        "digits": 5,
        "tick_size": Decimal("0.00001"),
        "contract_size": Decimal(100_000),
        "min_volume": Decimal("0.01"),
        "max_volume": Decimal(100),
        "volume_step": Decimal("0.01"),
        "base_currency": "EUR",
        "quote_currency": "USD",
        "margin_currency": "EUR",
    }
    fields.update(overrides)
    return Instrument(**fields)  # type: ignore[arg-type]


def intent(**overrides: object) -> OrderIntent:
    fields: dict[str, object] = {
        "symbol": "EURUSD",
        "side": Side.BUY,
        "order_type": OrderType.MARKET,
        "volume": Decimal("0.10"),
        "strategy_id": "test",
        "strategy_version": "1",
        "signal_ts": T0,
    }
    fields.update(overrides)
    return OrderIntent(**fields)  # type: ignore[arg-type]


def request(volume: Decimal = Decimal("1.00"), **overrides: object) -> OrderRequest:
    return intent(volume=volume, **overrides).to_request(new_client_order_id())


def new_order(volume: Decimal = Decimal("1.00")) -> Order:
    return Order.new(request(volume), T0)
