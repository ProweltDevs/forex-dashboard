"""ApiServer: the Control API served by uvicorn inside the trading process (same event loop).

uvicorn's ``Server.serve()`` normally installs its own SIGINT/SIGTERM handlers and re-raises the signal
after shutdown. ``trader run`` owns those signals (graceful shutdown of the whole app), so ``_Server``
leaves them alone; ``App.stop()`` stops the API first.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Generator, Mapping
from typing import TYPE_CHECKING, Any

import uvicorn

from trader.config.settings import ApiConfig
from trader.domain.errors import ConfigError
from trader.marketdata.bus import Bus
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


class _Server(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None]:
        yield  # trader run handles SIGINT/SIGTERM


class ApiServer:
    def __init__(self, app: App, config: ApiConfig, token: str) -> None:
        self._app = app
        self._config = config
        self._token = token
        self.hub: StreamHub | None = None
        self._server: _Server | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def port(self) -> int:
        """The bound port (useful with ``port: 0``)."""
        server = self._server
        if server is None or not server.started:
            raise RuntimeError("API server not started")
        return int(server.servers[0].sockets[0].getsockname()[1])

    async def start(self) -> None:
        app = self._app
        sources: list[Bus[Any]] = [a.oms.events for a in app.accounts.values()]
        sources += [b.portfolio.events for b in app.books.values()]
        sources.append(app.market.feeds)
        if app.persistence is not None:
            sources.append(app.persistence.writer.events)
        if app.runner is not None:
            sources.append(app.runner.events)
        hub = StreamHub(sources, app.market.ticks)
        await hub.start()
        self.hub = hub
        api = create_api(
            Control(app), self._token, hub, stream_tick_interval_ms=self._config.stream_tick_interval_ms
        )
        server = _Server(
            uvicorn.Config(
                api,
                host=self._config.host,
                port=self._config.port,
                http="h11",
                ws="websockets-sansio",
                lifespan="off",
                log_config=None,
                access_log=False,  # access lines carry query strings; RequestLog logs paths only
                server_header=False,
                timeout_graceful_shutdown=5,
            )
        )
        self._server = server
        self._task = asyncio.create_task(self._serve(server), name="api-server")
        while not server.started:
            if self._task.done():
                await self._task  # raises the startup error
                raise ConfigError("API server stopped during startup")
            await asyncio.sleep(0.02)
        if self._config.host not in _LOOPBACK:
            _log.warning(
                "api_exposed", host=self._config.host, detail="serve it only behind a TLS reverse proxy"
            )
        _log.info("api_started", host=self._config.host, port=self.port)

    async def _serve(self, server: _Server) -> None:
        try:
            await server.serve()
        except SystemExit:  # uvicorn exits on a bind failure; never let that end the trading process
            raise ConfigError(
                f"API server could not start on {self._config.host}:{self._config.port}"
            ) from None

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self.hub is not None:
            await self.hub.stop()  # ends every /stream connection
        if self._task is not None:
            with contextlib.suppress(Exception):
                await self._task
            self._task = None
        _log.info("api_stopped")
