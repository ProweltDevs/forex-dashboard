"""Broker registry: maps a config ``broker`` name to an adapter factory. The only place (besides the
adapters themselves) that knows which adapters exist."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from trader.config.settings import AccountConfig
from trader.domain.errors import ConfigError
from trader.domain.time import Clock

from .base import Broker, MarketDataSource, RateLimit
from .paper import PaperBroker, PaperSettings


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerDeps:
    """Runtime dependencies handed to factories by the composition root."""

    clock: Clock
    env: Mapping[str, str] = field(default_factory=dict)  # secrets are read from here by secrets_ref
    market_data: MarketDataSource | None = None  # required by the paper broker


type BrokerFactory = Callable[[AccountConfig, BrokerDeps], Broker]


class BrokerRegistry:
    def __init__(self) -> None:
        self._factories: dict[str, BrokerFactory] = {}

    def register(self, name: str, factory: BrokerFactory) -> None:
        if name in self._factories:
            raise ValueError(f"broker {name!r} is already registered")
        self._factories[name] = factory

    def names(self) -> frozenset[str]:
        return frozenset(self._factories)

    def create(self, account: AccountConfig, deps: BrokerDeps) -> Broker:
        factory = self._factories.get(account.broker)
        if factory is None:
            available = ", ".join(sorted(self._factories)) or "none"
            raise ConfigError(f"no adapter registered for broker {account.broker!r} (available: {available})")
        return factory(account, deps)


def paper_settings(account: AccountConfig) -> PaperSettings:
    cfg = account.paper
    if cfg is None:
        return PaperSettings(account_id=account.id)
    return PaperSettings(
        account_id=account.id,
        currency=cfg.currency,
        starting_balance=cfg.starting_balance,
        leverage=cfg.leverage,
        hedging=cfg.hedging,
        slippage_points=cfg.slippage_points,
        extra_spread_points=cfg.extra_spread_points,
        latency_ms=cfg.latency_ms,
        commission_per_lot=cfg.commission_per_lot,
        swap_long_per_lot=cfg.swap_long_per_lot,
        swap_short_per_lot=cfg.swap_short_per_lot,
        rollover_hour_utc=cfg.rollover_hour_utc,
        rate_limit=(
            RateLimit(name="requests", rate_per_s=cfg.rate_limit.rate_per_s, burst=cfg.rate_limit.burst)
            if cfg.rate_limit
            else None
        ),
    )


def _paper_factory(account: AccountConfig, deps: BrokerDeps) -> Broker:
    if deps.market_data is None:
        raise ConfigError(f"paper account {account.id!r} needs a market data source")
    return PaperBroker(deps.market_data, paper_settings(account), deps.clock, name="paper")


def default_registry() -> BrokerRegistry:
    """Registry with every adapter available in this build (exness/ctrader arrive in phases 2-3)."""
    registry = BrokerRegistry()
    registry.register("paper", _paper_factory)
    return registry
