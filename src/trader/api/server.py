"""ApiServer: the Control API served by uvicorn inside the trading process (same event loop).

It runs on ``EmbeddedServer`` (uvicorn without its signal capture: ``trader run`` owns SIGINT/SIGTERM).
``App.stop()`` stops the API first.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from trader.config.settings import ApiConfig
from trader.domain.errors import ConfigError
from trader.marketdata.bus import Bus
from trader.observability.http import EmbeddedServer
from trader.observability.logging import get_logger

from .control import Control
from .routes import create_api
from .stream import StreamHub

if TYPE_CHECKING:
    from trader.app import App

_log = get_logger("trader.api.server")

MIN_TOKEN_LENGTH = 32
_LOOPBACK = {"127.0.0.1", "::1", "localhost"}


def api_token(config: ApiConfig, env: Mapping[str, str]) -> str:
    token = env.get(config.token_env, "")
    if len(token) < MIN_TOKEN_LENGTH:
        raise ConfigError(
            f"api.enabled needs {config.token_env} with at least {MIN_TOKEN_LENGTH} characters "
            '(e.g. python -c "import secrets; print(secrets.token_urlsafe(32))")'
        )
    return token


class ApiServer:
    def __init__(self, app: App, config: ApiConfig, token: str) -> None:
        self._app = app
        self._config = config
        self._token = token
        self.hub: StreamHub | None = None
        self._server: EmbeddedServer | None = None

    @property
    def port(self) -> int:
        """The bound port (useful with ``port: 0``)."""
        if self._server is None:
            raise RuntimeError("API server not started")
        return self._server.port

    async def start(self) -> None:
        app = self._app
        sources: list[Bus[Any]] = [a.oms.events for a in app.accounts.values()]
        sources += [b.portfolio.events for b in app.books.values()]
        sources.append(app.market.feeds)
        if app.persistence is not None:
            sources.append(app.persistence.writer.events)
        if app.runner is not None:
            sources.append(app.runner.events)
        sources.append(app.ops_events)
        hub = StreamHub(sources, app.market.ticks)
        await hub.start()
        self.hub = hub
        api = create_api(
            Control(app), self._token, hub, stream_tick_interval_ms=self._config.stream_tick_interval_ms
        )
        self._server = EmbeddedServer(api, host=self._config.host, port=self._config.port, name="API")
        await self._server.start()
        if self._config.host not in _LOOPBACK:
            _log.warning(
                "api_exposed", host=self._config.host, detail="serve it only behind a TLS reverse proxy"
            )
        _log.info("api_started", host=self._config.host, port=self.port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.request_stop()
        if self.hub is not None:
            await self.hub.stop()  # ends every /stream connection
        if self._server is not None:
            await self._server.stop()
        _log.info("api_stopped")
