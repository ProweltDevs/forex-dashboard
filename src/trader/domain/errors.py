"""Canonical error hierarchy. Adapters translate broker error codes into these; broker-specific codes
never leave the adapter package (keep the raw code in ``broker_code`` for logs only)."""

from __future__ import annotations

from .enums import OrderStatus


class TraderError(Exception):
    """Base class for every platform error."""


class ConfigError(TraderError):
    """Invalid or unsafe configuration."""


class LiveTradingNotConfirmed(ConfigError):
    """Live mode requested without both TRADER_MODE=live and --confirm-live."""


class InvalidOrderTransition(TraderError):
    def __init__(self, current: OrderStatus, target: OrderStatus, client_order_id: str = "") -> None:
        self.current = current
        self.target = target
        self.client_order_id = client_order_id
        suffix = f" (order {client_order_id})" if client_order_id else ""
        super().__init__(f"illegal order transition {current} -> {target}{suffix}")


class BrokerError(TraderError):
    """Base for errors reported by or about a broker.

    ``retryable`` tells the OMS whether resubmitting with the SAME client_order_id is safe and useful.
    """

    retryable: bool = False

    def __init__(self, message: str = "", *, broker_code: str | None = None) -> None:
        self.broker_code = broker_code
        super().__init__(message or type(self).__name__)


class Requote(BrokerError):
    retryable = True


class MarketClosed(BrokerError):
    pass


class InsufficientMargin(BrokerError):
    pass


class RateLimited(BrokerError):
    retryable = True

    def __init__(
        self, message: str = "", *, broker_code: str | None = None, retry_after_ms: int | None = None
    ) -> None:
        super().__init__(message, broker_code=broker_code)
        self.retry_after_ms = retry_after_ms


class InvalidVolume(BrokerError):
    pass


class InvalidPrice(BrokerError):
    pass


class AuthExpired(BrokerError):
    retryable = True  # after re-auth


class BrokerUnavailable(BrokerError):
    retryable = True


class UnknownOrderState(BrokerError):
    """The outcome of a request is unknown (timeout, dropped connection). Never assume failure:
    the OMS moves the order to UNKNOWN and reconciles using the same client_order_id."""


class OrderRejected(BrokerError):
    """The broker rejected the order for a reason with no more specific mapping."""


class UnknownSymbol(BrokerError):
    """The symbol is not tradable on this account (not in the instrument list)."""


class OrderNotFound(BrokerError):
    """No open order with this ID (already filled, cancelled or never existed)."""


class PositionNotFound(BrokerError):
    """No open position with this ID."""


class ConversionUnavailable(TraderError):
    """No live tick is available to convert between two currencies."""
