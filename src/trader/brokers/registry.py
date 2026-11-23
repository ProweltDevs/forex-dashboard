"""Broker registry: maps a config ``broker`` name to an adapter factory. The only place (besides the
adapters themselves) that knows which adapters exist."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import SecretStr

from trader.config.secrets import load_secrets, require_private
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
    data_dir: Path = Path("data")  # encrypted token stores live under data_dir/secrets


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


CTRADER_SECRETS = ("client_id", "client_secret", "access_token", "refresh_token")


def _ctrader_factory(account: AccountConfig, deps: BrokerDeps) -> Broker:
    # Imported lazily so the protobuf bindings load only when a cTrader account is configured.
    from .ctrader import CTraderBroker, CTraderSettings, TokenSet, TokenStore
    from .ctrader.oauth import SECRETS_KEY_ENV

    if account.secrets_ref is None:  # guaranteed by AccountConfig validation
        raise ConfigError(f"account {account.id!r} needs a secrets_ref")
    if not account.account_id.isdigit():
        raise ConfigError(f"account {account.id!r}: account_id must be the numeric ctidTraderAccountId")
    secrets = load_secrets(account.secrets_ref, deps.env, required=CTRADER_SECRETS)
    key = deps.env.get(SECRETS_KEY_ENV)
    store = TokenStore(
        deps.data_dir / "secrets" / f"ctrader-{account.id}.tokens",
        SecretStr(key) if key else None,
    )
    tokens = store.load() or TokenSet(secrets["access_token"], secrets["refresh_token"], expires_at=None)
    opts = account.ctrader
    settings = CTraderSettings(
        account_id=account.id,
        ctid_trader_account_id=int(account.account_id),
        environment=account.environment,
        client_id=secrets["client_id"].get_secret_value(),
        client_secret=secrets["client_secret"],
        symbols=account.symbols,
        symbol_aliases=dict(opts.symbol_aliases) if opts else {},
        host=opts.host if opts else None,
        port=opts.port if opts else 5035,
        use_tls=opts.use_tls if opts else True,
        proxy=opts.proxy if opts else None,
    )
    return CTraderBroker(settings, tokens=tokens, store=store, clock=deps.clock)


def _exness_factory(account: AccountConfig, deps: BrokerDeps) -> Broker:
    from .exness import ExnessBroker, ExnessSettings

    if account.secrets_ref is None:  # guaranteed by AccountConfig validation
        raise ConfigError(f"account {account.id!r} needs a secrets_ref")
    if not account.account_id.isdigit():
        raise ConfigError(f"account {account.id!r}: account_id must be the numeric Exness trading account id")
    opts = account.exness
    if opts is None:
        raise ConfigError(
            f"account {account.id!r}: set exness.gateway_url (the DNS widget host in Key Management)"
        )
    secrets = load_secrets(account.secrets_ref, deps.env, required=("api_key",))
    private_key = secrets.get("private_key")
    key_file = deps.env.get(f"{account.secrets_ref}_PRIVATE_KEY_FILE")
    if private_key is None and key_file:
        path = Path(key_file).expanduser()
        if not path.is_file():
            raise ConfigError(f"{account.secrets_ref}_PRIVATE_KEY_FILE does not point to a file")
        require_private(path)
        private_key = SecretStr(path.read_text(encoding="utf-8"))
    if private_key is None:
        raise ConfigError(
            f"missing secrets: {account.secrets_ref}_PRIVATE_KEY (or {account.secrets_ref}_PRIVATE_KEY_FILE)"
        )
    confirm = f"{account.secrets_ref}_CONFIRM_DEMO_ACCOUNT"
    if account.environment == "demo" and deps.env.get(confirm, "").strip().lower() != "yes":
        # Exness serves demo and real accounts from the same hosts and no API field tells them apart
        # (docs/brokers/exness.md), so the `environment: demo` label alone cannot keep real money out of a
        # demo-mode run. Same guard as the demo tests.
        raise ConfigError(
            f"account {account.id!r}: set {confirm}=yes only after checking in the Exness Personal Area that "
            f"trading account {account.account_id} is a DEMO account (Exness hosts do not separate demo "
            "from real accounts)"
        )
    settings = ExnessSettings(
        account_id=account.id,
        exness_account_id=account.account_id,
        gateway_url=opts.gateway_url,
        api_key=secrets["api_key"].get_secret_value(),
        private_key=private_key,
        symbols=account.symbols,
        symbol_suffix=opts.symbol_suffix,
        symbol_aliases=dict(opts.symbol_aliases),
        hedging=opts.hedging,
    )
    return ExnessBroker(settings, clock=deps.clock)


def default_registry() -> BrokerRegistry:
    """Registry with every adapter available in this build."""
    registry = BrokerRegistry()
    registry.register("paper", _paper_factory)
    registry.register("ctrader", _ctrader_factory)
    registry.register("exness", _exness_factory)
    return registry
