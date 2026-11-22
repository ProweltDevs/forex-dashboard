"""Control API and notification startup rules: a token is required (and must be long), notification
channels need their secrets, and a failed API start shuts the app down gracefully."""

from __future__ import annotations

import asyncio
import socket
from datetime import timedelta
from pathlib import Path

import pytest

from tests.integration.api_helpers import TOKEN, api_config
from trader.app import App, run_app
from trader.config.settings import AppConfig
from trader.domain.errors import ConfigError
from trader.domain.time import SystemClock
from trader.strategy.base import StrategyStatus


def test_api_needs_a_long_token(tmp_path: Path) -> None:
    config = api_config(tmp_path)
    with pytest.raises(ConfigError, match="TRADER_API_TOKEN"):
        App(config, env={}, clock=SystemClock())
    with pytest.raises(ConfigError, match="at least 32"):
        App(config, env={"TRADER_API_TOKEN": "short"}, clock=SystemClock())
    disabled = AppConfig.model_validate(config.model_dump(mode="json", by_alias=True) | {"api": {}})
    assert App(disabled, env={}, clock=SystemClock()).api is None  # off by default


def test_notification_channels_need_their_secrets(tmp_path: Path) -> None:
    config = api_config(tmp_path, notify={"telegram": {}})
    with pytest.raises(ConfigError, match="TELEGRAM_BOT_TOKEN"):
        App(config, env={"TRADER_API_TOKEN": TOKEN}, clock=SystemClock())
    with pytest.raises(ConfigError, match="TELEGRAM_CHAT_ID"):
        App(config, env={"TRADER_API_TOKEN": TOKEN, "TELEGRAM_BOT_TOKEN": "1:x"}, clock=SystemClock())


def test_api_defaults_are_local() -> None:
    api = AppConfig.model_validate({"mode": "paper"}).api
    assert (api.enabled, api.host, api.port, api.token_env) == (False, "127.0.0.1", 8700, "TRADER_API_TOKEN")


async def test_a_taken_port_fails_startup_and_stops_the_app_gracefully(tmp_path: Path) -> None:
    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        port = busy.getsockname()[1]
        raw = api_config(tmp_path).model_dump(mode="json", by_alias=True)
        raw["api"]["port"] = port
        app = App(AppConfig.model_validate(raw), env={"TRADER_API_TOKEN": TOKEN}, clock=SystemClock())
        with pytest.raises(ConfigError, match=f"could not start on 127.0.0.1:{port}"):
            await run_app(app, asyncio.Event(), timedelta(seconds=5))
    assert app.runner is not None
    assert {h.status for h in app.runner.handles.values()} == {StrategyStatus.STOPPED}
    assert all(not s.broker.connected for s in app.accounts.values())
