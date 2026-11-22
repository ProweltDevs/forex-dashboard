"""Load YAML config and enforce the trading-mode safety rules."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import yaml
from pydantic import ValidationError

from trader.domain.enums import TradingMode
from trader.domain.errors import ConfigError, LiveTradingNotConfirmed

from .settings import AppConfig

MODE_ENV = "TRADER_MODE"


def load_config(path: Path) -> AppConfig:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}: {exc}") from exc
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: top level must be a mapping")
    try:
        return AppConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"invalid config {path}:\n{exc}") from exc


def resolve_mode(config: AppConfig, env: Mapping[str, str], *, confirm_live: bool) -> TradingMode:
    """Decide the effective trading mode and refuse anything unsafe.

    - live requires config ``mode: live`` AND env ``TRADER_MODE=live`` AND ``--confirm-live``.
    - env TRADER_MODE, when set, must agree with the config (no silent overrides).
    - paper mode may only trade paper accounts; demo mode may not contain live accounts.
    """
    env_value = env.get(MODE_ENV)
    env_mode: TradingMode | None = None
    if env_value:
        try:
            env_mode = TradingMode(env_value.strip().lower())
        except ValueError as exc:
            raise ConfigError(f"{MODE_ENV}={env_value!r} is not one of paper|demo|live") from exc
        if env_mode is not config.mode:
            raise ConfigError(f"{MODE_ENV}={env_mode} disagrees with config mode {config.mode}")

    mode = config.mode
    if mode is TradingMode.LIVE:
        if env_mode is not TradingMode.LIVE or not confirm_live:
            raise LiveTradingNotConfirmed(
                f"live trading requires {MODE_ENV}=live and the --confirm-live flag"
            )
        return mode

    if confirm_live:
        raise ConfigError("--confirm-live given but the config mode is not live")
    if config.has_live_accounts:
        raise ConfigError(f"mode {mode} must not contain environment: live accounts")
    if mode is TradingMode.PAPER:
        trading = [a.id for a in config.accounts if a.broker != "paper" and not _is_data_source(config, a.id)]
        if trading:
            raise ConfigError(
                f"paper mode can only trade paper accounts; non-paper accounts must be a market_data_from "
                f"source: {trading}"
            )
    return mode


def _is_data_source(config: AppConfig, account_id: str) -> bool:
    return any(a.paper is not None and a.paper.market_data_from == account_id for a in config.accounts)
