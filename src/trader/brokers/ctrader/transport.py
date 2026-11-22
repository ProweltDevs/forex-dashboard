"""asyncio TLS transport for cTrader Open API Protobuf (port 5035).

Framing (https://help.ctrader.com/open-api/sending-receiving-protobuf/): each frame is a 4-byte big-endian
length followed by a serialised ``ProtoMessage{payloadType, payload, clientMsgId}``.

- Requests get a unique ``clientMsgId``; the first inbound message carrying it resolves the request.
  Error responses (``ProtoErrorRes`` / ``ProtoOAErrorRes`` / ``ProtoOAOrderErrorEvent``) raise mapped errors.
- Every inbound message (including request responses) is also passed to ``on_message`` so the adapter
  can turn execution events into domain events.
- Heartbeat every 10 s (https://help.ctrader.com/open-api/connection/). If nothing arrives for
  ``idle_timeout`` the connection is treated as dead.
- Client-side rate limits: 50 req/s non-historical, 5 req/s historical, per connection
  (https://help.ctrader.com/open-api/). We stay a little under both.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import ssl
import struct
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import TypeVar
from urllib.parse import urlsplit

from google.protobuf.message import Message

from trader.brokers.base import RateLimit
from trader.brokers.ratelimit import TokenBucket
from trader.domain.errors import BrokerUnavailable, UnknownOrderState
from trader.observability.logging import get_logger

from .mapping import map_error
from .proto import OpenApiCommonMessages_pb2 as common
from .proto import OpenApiCommonModelMessages_pb2 as common_model
from .proto import OpenApiMessages_pb2 as msgs

_log = get_logger("trader.brokers.ctrader.transport")

M = TypeVar("M", bound=Message)

MAX_FRAME = 10 * 1024 * 1024
HEARTBEAT_S = 10.0

LIMIT_REQUESTS = RateLimit(name="requests", rate_per_s=Decimal(50), burst=50)
LIMIT_HISTORICAL = RateLimit(name="historical", rate_per_s=Decimal(5), burst=5)
# What we actually allow ourselves (headroom for clock skew between our bucket and the server's window).
_CLIENT_REQUESTS = RateLimit(name="requests", rate_per_s=Decimal(40), burst=10)
_CLIENT_HISTORICAL = RateLimit(name="historical", rate_per_s=Decimal(4), burst=2)


def _build_registry() -> dict[int, type[Message]]:
    registry: dict[int, type[Message]] = {}
    for module in (common, msgs):
        for name in module.DESCRIPTOR.message_types_by_name:
            cls: type[Message] = getattr(module, name)
            field = cls.DESCRIPTOR.fields_by_name.get("payloadType")
            if field is not None and field.has_default_value and name != "ProtoMessage":
                registry[int(field.default_value)] = cls
    return registry


PAYLOAD_TYPES: dict[int, type[Message]] = _build_registry()
_ERROR_TYPES = (common.ProtoErrorRes, msgs.ProtoOAErrorRes, msgs.ProtoOAOrderErrorEvent)


def payload_type(message: Message) -> int:
    return int(getattr(message, "payloadType"))  # noqa: B009 - every OA message has the field


def encode(message: Message, client_msg_id: str | None = None) -> bytes:
    wrapper = common.ProtoMessage(payloadType=payload_type(message), payload=message.SerializeToString())
    if client_msg_id is not None:
        wrapper.clientMsgId = client_msg_id
    body = wrapper.SerializeToString()
    return struct.pack(">I", len(body)) + body


@dataclass(frozen=True, slots=True)
class Inbound:
    message: Message
    client_msg_id: str | None


def decode(body: bytes) -> Inbound | None:
    wrapper = common.ProtoMessage()
    wrapper.ParseFromString(body)
    cls = PAYLOAD_TYPES.get(wrapper.payloadType)
    if cls is None:
        _log.debug("ctrader_unknown_payload", payload_type=wrapper.payloadType)
        return None
    message = cls()
    message.ParseFromString(wrapper.payload)
    return Inbound(message, wrapper.clientMsgId if wrapper.HasField("clientMsgId") else None)


def error_from(message: Message) -> Exception | None:
    if isinstance(message, msgs.ProtoOAErrorRes):
        return map_error(message.errorCode, message.description, retry_after_s=message.retryAfter or None)
    if isinstance(message, common.ProtoErrorRes | msgs.ProtoOAOrderErrorEvent):
        return map_error(message.errorCode, message.description)
    return None


async def _open(
    host: str, port: int, ssl_context: ssl.SSLContext | None, proxy: str | None, timeout: float
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    if proxy is None:
        return await asyncio.wait_for(
            asyncio.open_connection(
                host, port, ssl=ssl_context, server_hostname=host if ssl_context else None
            ),
            timeout,
        )
    # HTTP CONNECT tunnel, then TLS to the real host over it.
    parts = urlsplit(proxy)
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(parts.hostname, parts.port or 80), timeout
    )
    writer.write(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode())
    await writer.drain()
    status = await asyncio.wait_for(reader.readline(), timeout)
    if b" 200 " not in status:
        writer.close()
        raise BrokerUnavailable(
            f"proxy refused CONNECT {host}:{port}: {status.decode(errors='replace').strip()}"
        )
    while (await asyncio.wait_for(reader.readline(), timeout)) not in (b"\r\n", b""):
        pass
    if ssl_context is not None:
        await writer.start_tls(ssl_context, server_hostname=host)
    return reader, writer


class Transport:
    """One connection. Not reusable: after ``close`` (or a failure) create a new one."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        on_message: Callable[[Message], None],
        on_closed: Callable[[Exception | None], None],
        ssl_context: ssl.SSLContext | None = None,
        proxy: str | None = None,
        request_timeout: float = 10.0,
        connect_timeout: float = 10.0,
        heartbeat_s: float = HEARTBEAT_S,
        idle_timeout: float = 30.0,
        now_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self._host, self._port = host, port
        self._ssl = ssl_context
        self._proxy = proxy
        self._on_message = on_message
        self._on_closed = on_closed
        self._request_timeout = request_timeout
        self._connect_timeout = connect_timeout
        self._heartbeat_s = heartbeat_s
        self._idle_timeout = idle_timeout
        self._buckets = {
            False: TokenBucket(_CLIENT_REQUESTS, now_ns=now_ns),
            True: TokenBucket(_CLIENT_HISTORICAL, now_ns=now_ns),
        }
        self._ids = itertools.count(1)
        self._pending: dict[str, asyncio.Future[Message]] = {}
        self._writer: asyncio.StreamWriter | None = None
        self._send_lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []
        self._last_rx = time.monotonic()
        self._closed = False

    @property
    def is_open(self) -> bool:
        return self._writer is not None and not self._closed

    async def connect(self) -> None:
        try:
            reader, writer = await _open(
                self._host, self._port, self._ssl, self._proxy, self._connect_timeout
            )
        except (OSError, TimeoutError) as exc:
            raise BrokerUnavailable(f"cannot connect to {self._host}:{self._port}: {exc}") from exc
        self._writer = writer
        self._last_rx = time.monotonic()
        self._tasks = [
            asyncio.create_task(self._read_loop(reader), name="ctrader-read"),
            asyncio.create_task(self._heartbeat_loop(), name="ctrader-heartbeat"),
        ]

    async def close(self) -> None:
        await self._shutdown(None)

    async def send(self, message: Message) -> None:
        """Fire-and-forget (heartbeats). Not rate limited."""
        await self._write(encode(message))

    async def request(
        self, message: Message, response: type[M], *, historical: bool = False, timeout: float | None = None
    ) -> M:
        """Send ``message`` and wait for the response carrying the same clientMsgId.

        Raises the mapped broker error for error responses, ``BrokerUnavailable`` if the connection
        drops first, and ``UnknownOrderState`` on timeout (the request may or may not have executed)."""
        await self._buckets[historical].acquire()
        if not self.is_open:
            raise BrokerUnavailable("not connected")
        msg_id = f"c{next(self._ids)}"
        future: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = future
        try:
            await self._write(encode(message, msg_id))
            result = await asyncio.wait_for(future, timeout or self._request_timeout)
        except TimeoutError as exc:
            raise UnknownOrderState(f"no response to {type(message).__name__} within timeout") from exc
        finally:
            self._pending.pop(msg_id, None)
        if (error := error_from(result)) is not None:
            raise error
        if not isinstance(result, response):
            raise BrokerUnavailable(
                f"unexpected response {type(result).__name__} to {type(message).__name__}"
            )
        return result

    # ------------------------------------------------------------------ internals

    async def _write(self, frame: bytes) -> None:
        writer = self._writer
        if writer is None or self._closed:
            raise BrokerUnavailable("not connected")
        async with self._send_lock:  # one writer at a time; frames must not interleave
            try:
                writer.write(frame)
                await writer.drain()
            except (OSError, ConnectionError) as exc:
                await self._shutdown(exc)
                raise BrokerUnavailable(f"send failed: {exc}") from exc

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        error: Exception | None = None
        try:
            while True:
                header = await reader.readexactly(4)
                (length,) = struct.unpack(">I", header)
                if length > MAX_FRAME:
                    raise BrokerUnavailable(f"frame too large: {length} bytes")
                body = await reader.readexactly(length) if length else b""
                self._last_rx = time.monotonic()
                if not body:
                    continue
                inbound = decode(body)
                if inbound is None:
                    continue
                if inbound.client_msg_id is not None:
                    future = self._pending.get(inbound.client_msg_id)
                    if future is not None and not future.done():
                        future.set_result(inbound.message)
                if isinstance(inbound.message, common.ProtoHeartbeatEvent):
                    continue
                try:
                    self._on_message(inbound.message)
                except Exception:
                    _log.exception("ctrader_handler_error", message=type(inbound.message).__name__)
        except asyncio.IncompleteReadError:
            error = BrokerUnavailable("connection closed by server")
        except asyncio.CancelledError:
            return
        except Exception as exc:
            error = exc
        asyncio.get_running_loop().create_task(self._shutdown(error))

    async def _heartbeat_loop(self) -> None:
        heartbeat = common.ProtoHeartbeatEvent(payloadType=common_model.HEARTBEAT_EVENT)
        while True:
            await asyncio.sleep(self._heartbeat_s)
            if time.monotonic() - self._last_rx > self._idle_timeout:
                await self._shutdown(BrokerUnavailable(f"no data for {self._idle_timeout:.0f}s"))
                return
            with contextlib.suppress(BrokerUnavailable):
                await self.send(heartbeat)

    async def _shutdown(self, error: Exception | None) -> None:
        if self._closed:
            return
        self._closed = True
        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current:
                task.cancel()
        for future in self._pending.values():
            if not future.done():  # the request was written: its outcome is unknown, never assume failure
                future.set_exception(UnknownOrderState("connection lost before the response arrived"))
        self._pending.clear()
        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._writer.wait_closed(), 2)
        self._on_closed(error)
