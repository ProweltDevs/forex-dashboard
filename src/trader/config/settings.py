"""Configuration models (pydantic v2). Secrets never live in YAML: accounts reference them by
``secrets_ref`` and they are read from the environment (see ``trader.config.secrets``)."""

from __future__ import annotations

import re
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trader.domain.enums import Timeframe, TradingMode

from .risk import RiskConfig

BrokerName = Literal["paper", "exness", "ctrader"]
Environment = Literal["demo", "live"]

_SECRETS_REF = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")
_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9._]{1,31}$")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class LoggingConfig(_Model):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    json_output: bool = Field(default=True, alias="json")


class DatabaseConfig(_Model):
    # Credentials come from the environment (TRADER_DB_URL), never from YAML.
    url_env: str = "TRADER_DB_URL"
    pool_size: int = Field(default=5, ge=1, le=50)


class PaperRateLimitConfig(_Model):
    rate_per_s: Decimal = Field(gt=0)
    burst: int = Field(ge=1)


class SyntheticFeedConfig(_Model):
    """Seeded random-walk prices (NOT market data): lets paper mode run without broker credentials."""

    seed: int = 1
    tick_interval_ms: int = Field(default=250, ge=10)
    volatility: float = Field(default=0.00005, gt=0, le=0.01)  # per-tick stdev of the log return


class PaperConfig(_Model):
    """Fill simulation for the paper broker (see ``trader.brokers.paper.PaperSettings``)."""

    starting_balance: Decimal = Field(default=Decimal(10_000), ge=0)
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
    leverage: int = Field(default=100, ge=1, le=3000)
    hedging: bool = True
    slippage_points: int = Field(default=0, ge=0)
    extra_spread_points: int = Field(default=0, ge=0)
    latency_ms: int = Field(default=0, ge=0)
    commission_per_lot: Decimal = Field(default=Decimal(0), ge=0)
    swap_long_per_lot: Decimal = Decimal(0)
    swap_short_per_lot: Decimal = Decimal(0)
    rollover_hour_utc: int = Field(default=21, ge=0, le=23)
    rate_limit: PaperRateLimitConfig | None = None
    market_data_from: str | None = None  # id of another account to source live ticks from
    synthetic_feed: SyntheticFeedConfig | None = None  # random-walk prices instead (no credentials needed)

    @model_validator(mode="after")
    def _one_source(self) -> Self:
        if self.market_data_from is not None and self.synthetic_feed is not None:
            raise ValueError("set either market_data_from or synthetic_feed, not both")
        return self


class CTraderOptions(_Model):
    """cTrader-specific account options. Credentials are NOT here: they come from ``secrets_ref`` env vars."""

    symbol_aliases: dict[str, str] = Field(default_factory=dict)  # broker symbol name -> canonical
    proxy: str | None = None  # http://host:port CONNECT proxy, only if your network requires one
    host: str | None = None  # override the demo/live endpoint (tests)
    port: int = Field(default=5035, ge=1, le=65535)
    use_tls: bool = True


class ExnessOptions(_Model):
    """Exness-specific account options. Credentials come from ``secrets_ref`` env vars, not from here."""

    gateway_url: str = Field(pattern=r"^https://[^/?#\s]+/?$")  # from the DNS widget in Key Management
    symbol_suffix: str = ""  # e.g. "m" when instruments are named "EURUSDm"
    symbol_aliases: dict[str, str] = Field(default_factory=dict)  # broker instrument name -> canonical
    hedging: bool = True


class AccountConfig(_Model):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    broker: BrokerName
    environment: Environment = "demo"
    account_id: str = ""  # broker account ID (not secret)
    secrets_ref: str | None = None  # env-var prefix, e.g. CTRADER_DEMO -> CTRADER_DEMO_CLIENT_ID
    symbols: tuple[str, ...] = ()
    paper: PaperConfig | None = None
    ctrader: CTraderOptions | None = None
    exness: ExnessOptions | None = None
    risk: RiskConfig = RiskConfig()

    @field_validator("secrets_ref")
    @classmethod
    def _check_ref(cls, value: str | None) -> str | None:
        if value is not None and not _SECRETS_REF.fullmatch(value):
            raise ValueError("secrets_ref must be an UPPER_SNAKE env-var prefix")
        return value

    @field_validator("symbols")
    @classmethod
    def _check_symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        bad = [s for s in value if not _SYMBOL.fullmatch(s)]
        if bad:
            raise ValueError(f"invalid canonical symbols: {bad}")
        if len(set(value)) != len(value):
            raise ValueError("duplicate symbols")
        return value

    @model_validator(mode="after")
    def _check_broker_fields(self) -> Self:
        # Each broker-specific options block is only valid for its own broker.
        for key in ("paper", "ctrader", "exness"):
            if getattr(self, key) is not None and self.broker != key:
                raise ValueError(f"'{key}' settings are only valid for broker: {key}")
        if self.broker == "paper":
            if self.environment == "live":
                raise ValueError("paper accounts cannot have environment: live")
        else:
            if not self.secrets_ref:
                raise ValueError(f"{self.broker} accounts need a secrets_ref")
            if not self.account_id:
                raise ValueError(f"{self.broker} accounts need an account_id")
        return self


class StaleFeedConfig(_Model):
    min_silence_s: int = Field(default=15, ge=1)
    max_silence_s: int = Field(default=120, ge=1)
    multiplier: int = Field(default=20, ge=1)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.max_silence_s < self.min_silence_s:
            raise ValueError("max_silence_s must be >= min_silence_s")
        return self


class RecorderConfig(_Model):
    enabled: bool = True  # always on in production; tests may disable it
    batch_size: int = Field(default=5_000, ge=1)
    flush_interval_s: int = Field(default=10, ge=1)


class MarketDataConfig(_Model):
    timeframes: tuple[Timeframe, ...] = (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1)
    price_source: Literal["bid", "ask", "mid"] = "bid"  # broker candles are bid-based
    # Trading-day alignment in minutes: -120 = days and weeks start at 22:00 UTC (NY close, or the midnight
    # of a UTC+2 broker server); 0 = UTC midnight. Bars at every timeframe shift with it.
    day_offset_minutes: int = Field(default=0, ge=-720, le=720)
    bar_history: int = Field(default=1_000, ge=1)
    bar_grace_ms: int = Field(default=2_000, ge=0)
    warmup_bars: int = Field(default=300, ge=0)
    stale: StaleFeedConfig = StaleFeedConfig()
    recorder: RecorderConfig = RecorderConfig()

    @field_validator("timeframes")
    @classmethod
    def _check_tfs(cls, value: tuple[Timeframe, ...]) -> tuple[Timeframe, ...]:
        if not value:
            raise ValueError("at least one timeframe")
        if len(set(value)) != len(value):
            raise ValueError("duplicate timeframes")
        return value


_STRATEGY_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


class StrategyConfig(_Model):
    """One strategy instance (SPEC §8). Several instances of one class may run with different params."""

    id: str  # instance id: the strategy_id stamped on every order
    class_: str = Field(alias="class")  # built-in name (e.g. ema_cross) or "package.module:ClassName"
    account: str  # trading account id
    symbols: tuple[str, ...]
    timeframes: tuple[Timeframe, ...] = ()
    params: dict[str, Any] = Field(default_factory=dict)  # validated by the strategy's Params model
    enabled: bool = True
    max_errors: int = Field(default=5, ge=1)  # errors before this strategy is auto-paused
    callback_budget_ms: float = Field(default=50.0, gt=0)  # slower callbacks are logged
    seed: int = 0  # ctx.random seed
    warmup_bars: int | None = Field(default=None, ge=0)  # default: the class's warmup_bars

    @field_validator("id")
    @classmethod
    def _check_id(cls, value: str) -> str:
        if not _STRATEGY_ID.fullmatch(value):
            raise ValueError("strategy id must be lowercase letters, digits, '-' or '_' (max 64)")
        return value

    @field_validator("symbols")
    @classmethod
    def _check_symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("a strategy needs at least one symbol")
        return value


class PortfolioConfig(_Model):
    """P&L tracking (SPEC §9): equity sampling and the computed-vs-broker drift check."""

    equity_sample_s: int = Field(default=60, ge=1)  # plus a sample on every fill
    drift_pct: Decimal = Field(default=Decimal("0.5"), gt=0)  # of broker equity
    drift_min: Decimal = Field(default=Decimal("1"), ge=0)  # account currency: never alert below this
    balance_grace_s: int = Field(default=10, ge=1)

    @property
    def equity_sample(self) -> timedelta:
        return timedelta(seconds=self.equity_sample_s)

    @property
    def balance_grace(self) -> timedelta:
        return timedelta(seconds=self.balance_grace_s)


class ApiConfig(_Model):
    """Control API (SPEC §13). Localhost by default: expose it only behind a TLS reverse proxy."""

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = Field(default=8700, ge=0, le=65535)  # 0: any free port (tests)
    token_env: str = "TRADER_API_TOKEN"  # bearer token, from the environment only
    stream_tick_interval_ms: int = Field(default=500, ge=50)  # per-client tick throttle on /stream


type SeverityName = Literal["info", "warning", "critical"]


class TelegramConfig(_Model):
    token_env: str = "TELEGRAM_BOT_TOKEN"
    chat_id_env: str = "TELEGRAM_CHAT_ID"  # a private chat: commands are refused from groups
    api_base: str = "https://api.telegram.org"  # or a self-hosted Bot API server
    min_severity: SeverityName = "info"
    commands: bool = False  # accept /status /pnl /pause /kill from that chat


class EmailConfig(_Model):
    host: str
    port: int = Field(default=587, ge=1, le=65535)
    starttls: bool = True
    sender: str
    recipients: tuple[str, ...]
    user_env: str = "TRADER_SMTP_USER"
    password_env: str = "TRADER_SMTP_PASSWORD"
    min_severity: SeverityName = "warning"


class NotifyConfig(_Model):
    """Notifications (SPEC §14): Telegram (primary) and email (optional)."""

    telegram: TelegramConfig | None = None
    email: EmailConfig | None = None
    dedupe_s: int = Field(default=300, ge=0)  # same alert key within this window: sent once
    rate_per_min: int = Field(default=20, ge=1)  # per channel; extras are summarised later
    fills: bool = True  # notify every fill (off for very active strategies)


class AppConfig(_Model):
    mode: TradingMode = TradingMode.PAPER
    data_dir: Path = Path("data")
    logging: LoggingConfig = LoggingConfig()
    database: DatabaseConfig | None = None
    market_data: MarketDataConfig = MarketDataConfig()
    portfolio: PortfolioConfig = PortfolioConfig()
    api: ApiConfig = ApiConfig()
    notify: NotifyConfig = NotifyConfig()
    accounts: tuple[AccountConfig, ...] = ()
    strategies: tuple[StrategyConfig, ...] = ()

    @model_validator(mode="after")
    def _check_strategies(self) -> Self:
        ids = [s.id for s in self.strategies]
        if len(set(ids)) != len(ids):
            raise ValueError("strategy ids must be unique")
        accounts = {a.id: a for a in self.accounts}
        sources = {a.paper.market_data_from for a in self.accounts if a.paper and a.paper.market_data_from}
        for st in self.strategies:
            acc = accounts.get(st.account)
            if acc is None:
                raise ValueError(f"strategy {st.id}: unknown account {st.account!r}")
            if acc.id in sources:
                raise ValueError(f"strategy {st.id}: {acc.id} is a market-data source, not a trading account")
            missing = [s for s in st.symbols if s not in acc.symbols]
            if missing:
                raise ValueError(f"strategy {st.id}: symbols {missing} are not in account {acc.id}'s symbols")
            tfs = [tf for tf in st.timeframes if tf not in self.market_data.timeframes]
            if tfs:
                raise ValueError(f"strategy {st.id}: timeframes {tfs} are not in market_data.timeframes")
        return self

    @model_validator(mode="after")
    def _check_accounts(self) -> Self:
        ids = [a.id for a in self.accounts]
        if len(set(ids)) != len(ids):
            raise ValueError("account ids must be unique")
        known = set(ids)
        for acc in self.accounts:
            src = acc.paper.market_data_from if acc.paper else None
            if src is not None and src not in known:
                raise ValueError(f"account {acc.id}: market_data_from {src!r} is not a configured account")
        return self

    @property
    def has_live_accounts(self) -> bool:
        return any(a.environment == "live" for a in self.accounts)
