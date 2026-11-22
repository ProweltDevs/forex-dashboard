"""Signed WebSocket streams (https://www.exness-api.com/trading/get-started/subscribe-to-websocket-events).

The handshake is signed as a ``GET`` of the exact connection path. Two streams per account:
``/ws/events`` (transactions, account_state, instruments, hmr) and ``/ws/ticks``. Streams have no
durable replay: on any close we report it and the adapter reconnects, resubscribes and reconciles.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Callable
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from trader.domain.errors import BrokerError, BrokerUnavailable
from trader.observability.logging import get_logger

from .auth import Signer
from .mapping import Json, map_error
from .rest import USER_AGENT

_log = get_logger("trader.brokers.exness.ws")


def ws_base(http_base: str) -> str:
    if http_base.startswith("https://"):
        return "wss://" + http_base[len("https://") :]
    if http_base.startswith("http://"):
        return "ws://" + http_base[len("http://") :]
    raise ValueError(f"not an http(s) base URL: {http_base!r}")


def _handshake_error(exc: InvalidStatus) -> BrokerError:
    status = exc.response.status_code
    try:
        data = json.loads(exc.response.body or b"{}")
    except ValueError:
        data = {}
    if isinstance(data, dict) and isinstance(data.get("error_message"), str):
        return map_error(data["error_message"])
    return BrokerUnavailable(f"WebSocket handshake rejected: HTTP {status}")


class Stream:
    """One WebSocket connection. Not reusable: after it closes, create a new one."""

    def __init__(
        self,
        base: str,
        path: str,
        signer: Signer,
        *,
        on_message: Callable[[Json], None],
        on_closed: Callable[[Exception | None], None],
        proxy: str | bool | None = True,
        subprotocol: str | None = "exness-ws-protocol",
        open_timeout: float = 10.0,
        ping_interval: float = 20.0,
    ) -> None:
        self._url = ws_base(base) + path
        self._path = path
        self._signer = signer
        self._on_message = on_message
        self._on_closed = on_closed
        self._proxy = proxy
        self._subprotocol = subprotocol
        self._open_timeout = open_timeout
        self._ping_interval = ping_interval
        self._ws: ClientConnection | None = None
        self._reader: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def is_open(self) -> bool:
        return self._ws is not None and not self._closed

    async def connect(self) -> None:
        headers = self._signer.headers("GET", self._path)
        try:
            self._ws = await connect(
                self._url,
                additional_headers=headers,
                user_agent_header=USER_AGENT,
                subprotocols=[self._subprotocol] if self._subprotocol else None,  # type: ignore[list-item]
                proxy=self._proxy,  # type: ignore[arg-type]
                open_timeout=self._open_timeout,
                ping_interval=self._ping_interval,
                max_size=2**24,
            )
        except InvalidStatus as exc:
            raise _handshake_error(exc) from exc
        except (OSError, TimeoutError, WebSocketException) as exc:
            raise BrokerUnavailable(f"WebSocket connect to {self._path} failed: {exc}") from exc
        self._reader = asyncio.create_task(self._read(self._ws), name=f"exness-ws{self._path[-7:]}")

    async def send(self, message: Json) -> None:
        if self._ws is None or self._closed:
            raise BrokerUnavailable("WebSocket is not connected")
        try:
            await self._ws.send(json.dumps(message, separators=(",", ":")))
        except ConnectionClosed as exc:
            raise BrokerUnavailable(f"WebSocket closed: {exc}") from exc

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._reader is not None and self._reader is not asyncio.current_task():
            self._reader.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reader
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._ws.close(), 2)

    async def _read(self, ws: ClientConnection) -> None:
        error: Exception | None = None
        try:
            async for raw in ws:
                try:
                    message: Any = json.loads(raw)
                except ValueError:
                    _log.warning("exness_ws_bad_json", path=self._path)
                    continue
                if isinstance(message, dict):
                    try:
                        self._on_message(message)
                    except Exception:
                        _log.exception("exness_ws_handler_error", path=self._path)
            error = BrokerUnavailable(f"WebSocket {self._path} closed by server")
        except asyncio.CancelledError:
            return
        except ConnectionClosed as exc:
            error = BrokerUnavailable(f"WebSocket {self._path} closed: {exc}")
        except Exception as exc:
            error = exc
        if not self._closed:
            self._closed = True
            self._on_closed(error)
