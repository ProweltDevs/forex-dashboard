"""Configuration models (pydantic v2). Secrets never live in YAML: accounts reference them by
``secrets_ref`` and they are read from the environment (see ``trader.config.secrets``)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trader.domain.enums import TradingMode

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


class PaperConfig(_Model):
    """Fill simulation for the paper broker."""

    starting_balance: str = "10000"
    currency: str = "USD"
    leverage: int = Field(default=100, ge=1, le=3000)
    slippage_points: int = Field(default=0, ge=0)
    latency_ms: int = Field(default=0, ge=0)
    commission_per_lot: str = "0"
    market_data_from: str | None = None  # id of another account to source live ticks from


class AccountConfig(_Model):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    broker: BrokerName
    environment: Environment = "demo"
    account_id: str = ""  # broker account ID (not secret)
    secrets_ref: str | None = None  # env-var prefix, e.g. CTRADER_DEMO -> CTRADER_DEMO_CLIENT_ID
    symbols: tuple[str, ...] = ()
    paper: PaperConfig | None = None

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
        if self.broker == "paper":
            if self.environment == "live":
                raise ValueError("paper accounts cannot have environment: live")
        else:
            if self.paper is not None:
                raise ValueError("'paper' settings are only valid for broker: paper")
            if not self.secrets_ref:
                raise ValueError(f"{self.broker} accounts need a secrets_ref")
            if not self.account_id:
                raise ValueError(f"{self.broker} accounts need an account_id")
        return self


class AppConfig(_Model):
    mode: TradingMode = TradingMode.PAPER
    data_dir: Path = Path("data")
    logging: LoggingConfig = LoggingConfig()
    database: DatabaseConfig | None = None
    accounts: tuple[AccountConfig, ...] = ()

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
