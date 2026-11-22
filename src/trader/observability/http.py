"""EmbeddedServer: an ASGI app served by uvicorn inside the trading process (same event loop), for the
Control API and the ops endpoints.

uvicorn's ``Server.serve()`` normally installs its own SIGINT/SIGTERM handlers and re-raises the signal
after shutdown. ``trader run`` owns those signals (graceful shutdown of the whole app), so this server
leaves them alone. A bind failure (uvicorn calls ``sys.exit``) becomes a ``ConfigError`` instead of ending
the process.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Generator

import uvicorn
from starlette.types import ASGIApp

from trader.domain.errors import ConfigError


class _Server(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None]:
        yield  # trader run handles SIGINT/SIGTERM


class EmbeddedServer:
    def __init__(self, app: ASGIApp, *, host: str, port: int, name: str) -> None:
        self.host = host
        self.name = name
        self._server = _Server(
            uvicorn.Config(
                app,
                host=host,
                port=port,
                http="h11",
                ws="websockets-sansio",
                lifespan="off",
                log_config=None,
                access_log=False,  # access lines carry query strings; callers log paths only
                server_header=False,
                timeout_graceful_shutdown=5,
            )
        )
        self._requested_port = port
        self._task: asyncio.Task[None] | None = None

    @property
    def port(self) -> int:
        """The bound port (useful with ``port: 0``)."""
        if not self._server.started:
            raise RuntimeError(f"{self.name} server not started")
        return int(self._server.servers[0].sockets[0].getsockname()[1])

    async def start(self) -> None:
        self._task = asyncio.create_task(self._serve(), name=f"{self.name}-server")
        while not self._server.started:
            if self._task.done():
                await self._task  # raises the startup error
                raise ConfigError(f"{self.name} server stopped during startup")
            await asyncio.sleep(0.02)

    async def _serve(self) -> None:
        try:
            await self._server.serve()
        except SystemExit:  # uvicorn exits on a bind failure; never let that end the trading process
            raise ConfigError(
                f"{self.name} server could not start on {self.host}:{self._requested_port}"
            ) from None

    def request_stop(self) -> None:
        self._server.should_exit = True

    async def stop(self) -> None:
        self.request_stop()
        if self._task is not None:
            with contextlib.suppress(Exception):
                await self._task
            self._task = None
