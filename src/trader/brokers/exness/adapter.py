"""Exness Trader API adapter. Protocol facts: docs/brokers/exness.md. Design: docs/adr/0005-exness-adapter.md.

Flow: Gateway discovery -> Access Point -> account details + limits + instrument conditions -> events
WebSocket (``transactions`` + ``account_state``; the first message is a ``trading_state_snapshot``) ->
ticks WebSocket. Mutating requests return an ACK (``operation_id``); results arrive as
``transaction_event`` correlated by ``client_request_id`` (= our idempotency key, = ``client_order_id``
for new orders). Missing results are recovered by polling operation status, then resubscribing.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from collections.abc import AsyncIterator, Coroutine, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal

import httpx
from pydantic import SecretStr

from trader.brokers.base import BrokerAck, Capabilities, OrderChanges, RateLimits
from trader.domain.enums import (
    ConnectionState,
    OrderStatus,
    OrderType,
    PositionChange,
    Side,
    Timeframe,
    TimeInForce,
)
from trader.domain.errors import (
    BrokerError,
    BrokerUnavailable,
    InvalidVolume,
    OrderNotFound,
    OrderRejected,
    PositionNotFound,
    UnknownSymbol,
)
from trader.domain.events import (
    AccountEvent,
    ConnectionEvent,
    DomainEvent,
    FillEvent,
    OrderEvent,
    PositionEvent,
    TickEvent,
)
from trader.domain.ids import new_client_order_id
from trader.domain.models import (
    AccountState,
    Bar,
    BrokerSnapshot,
    Fill,
    Instrument,
    Order,
    OrderRequest,
    Position,
    StatusChange,
)
from trader.domain.money import ZERO
from trader.domain.time import Clock, ensure_utc
from trader.observability.logging import get_logger

from . import mapping as mp
from .auth import Signer, is_valid_idempotency_key, load_signing_key
from .discovery import resolve_access_point
from .mapping import Json, Tag
from .rest import RequestLimits, RestClient
from .ws import Stream

_log = get_logger("trader.brokers.exness")

_OPEN_KINDS = frozenset({"open", "pending"})
_HISTORY_MAX_PAGES = 50  # x 200 records per page


@dataclass(frozen=True, slots=True, kw_only=True)
class ExnessSettings:
    account_id: str  # our config id
    exness_account_id: str  # numeric trading account id
    gateway_url: str  # from the DNS widget in Key Management
    api_key: str = field(repr=False)  # sent in a header on every request: keep it out of reprs and logs
    private_key: SecretStr  # 128 hex chars or PKCS#8 PEM
    symbols: tuple[str, ...] = ()  # canonical; empty = every available instrument (one request each)
    symbol_suffix: str = ""  # e.g. "m" if your account's instruments are named "EURUSDm"
    symbol_aliases: Mapping[str, str] = field(default_factory=dict)  # broker name -> canonical
    hedging: bool = True
    comment_max_len: int = 31
    request_timeout_s: float = 10.0
    operation_timeout_s: float = 15.0  # wait this long for a transaction_event, then poll operation status
    snapshot_timeout_s: float = 15.0
    reconnect_initial_s: float = 0.5
    reconnect_max_s: float = 30.0
    trust_env: bool = True  # use HTTPS_PROXY etc. for REST and WebSockets
    allow_insecure: bool = False  # tests only: http/ws to a local fake server
    ws_subprotocol: str | None = "exness-ws-protocol"

    def __post_init__(self) -> None:
        if not self.exness_account_id.isdigit():
            raise ValueError("exness_account_id must be the numeric trading account id")


@dataclass(slots=True)
class _Pending:
    kind: str  # open | pending | modify | cancel | sltp | close
    request: OrderRequest | None
    sent_mono: float
    op_id: str | None = None
    done: bool = False


class ExnessBroker:
    def __init__(
        self,
        settings: ExnessSettings,
        *,
        clock: Clock,
        http_transport: httpx.AsyncBaseTransport | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._s = settings
        self._clock = clock
        self._signer = Signer(settings.api_key, load_signing_key(settings.private_key))
        self._http_transport = http_transport
        self._rng = rng or random.Random()
        self._limits = RequestLimits()
        self._http: httpx.AsyncClient | None = None
        self._ap: str | None = None
        self._rest: RestClient | None = None
        self._events_ws: Stream | None = None
        self._ticks_ws: Stream | None = None
        self._snapshot_ready: asyncio.Future[None] | None = None

        self._connected = False
        self._closing = False
        self._awaiting_snapshot = True
        self._events: asyncio.Queue[DomainEvent | None] = asyncio.Queue()
        self._events_consumed = False
        self._tasks: set[asyncio.Task[None]] = set()
        self._reconnect_task: asyncio.Task[None] | None = None
        self._ticks_task: asyncio.Task[None] | None = None
        self._watchdog: asyncio.Task[None] | None = None

        self._details: Json = {}
        self._symbols: dict[str, mp.SymbolInfo] = {}
        self._by_name: dict[str, mp.SymbolInfo] = {}
        self._subscribed: set[str] = set()
        self._quotes: dict[str, tuple[Decimal, Decimal]] = {}
        # broker-state caches (raw JSON, keyed by Exness ids)
        self._orders: dict[str, Json] = {}  # active limit/stop orders
        self._finished: dict[str, Json] = {}  # recently finished orders (for find_order), bounded
        self._sltp: dict[str, Json] = {}  # position_id -> active sltp order
        self._positions: dict[str, Json] = {}
        self._account_state: Json = {}
        # what only we know (Exness does not echo comments or client ids on entities)
        self._tags: dict[str, Tag] = {}  # client_order_id -> tag
        self._order_tag: dict[str, Tag] = {}  # order_id -> tag
        self._pos_strategy: dict[str, str] = {}  # position_id -> strategy_id
        self._pending: dict[str, _Pending] = {}  # idempotency key -> request in flight
        self._op_key: dict[str, str] = {}  # operation_id -> idempotency key

    # ------------------------------------------------------------------ identity

    @property
    def name(self) -> str:
        return "exness"

    @property
    def capabilities(self) -> Capabilities:
        return Capabilities(
            order_types=frozenset({OrderType.MARKET, OrderType.LIMIT, OrderType.STOP}),
            native_sl_tp=True,
            native_trailing_stop=False,
            partial_close=True,
            close_by=False,  # supported by the API (close_by_position_id) but not exposed by Broker yet
            hedging=self._s.hedging,
            netting=not self._s.hedging,
            modify_pending=True,  # price and SL/TP; not volume
            server_side_expiry=False,  # no expiry field on pending orders: the OMS emulates GTD/DAY
            tick_stream=True,
            candle_history=dict(mp.HISTORY_DEPTH),
            max_candles_per_request=self._limits.candles_per_request,
            sync_order_result=False,
            idempotent_submit=True,  # same key + payload -> the original operation, not executed again
        )

    def _acct(self) -> str:
        return self._s.exness_account_id

    def _now(self) -> datetime:
        return self._clock.now()

    # ------------------------------------------------------------------ lifecycle

    async def connect(self) -> None:
        if self._connected:
            return
        self._closing = False
        await self._establish()
        self._set_connection(ConnectionState.UP, "connected")
        if self._watchdog is None:
            self._watchdog = asyncio.create_task(self._watch_operations(), name="exness-op-watchdog")

    async def close(self) -> None:
        self._closing = True
        for task in [t for t in (self._reconnect_task, self._ticks_task, self._watchdog, *self._tasks) if t]:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._reconnect_task = self._ticks_task = self._watchdog = None
        for stream in (self._events_ws, self._ticks_ws):
            if stream is not None:
                await stream.close()
        self._events_ws = self._ticks_ws = None
        if self._http is not None:
            await self._http.aclose()
            self._http = None
        if self._connected:
            self._set_connection(ConnectionState.DOWN, "closed")
        self._events.put_nowait(None)

    async def drop_connection(self) -> None:
        """Test hook: drop the events stream as a network failure would; the adapter reconnects itself."""
        stream = self._events_ws
        if stream is not None:
            await stream.close()
            self._on_events_closed(stream, BrokerUnavailable("dropped by test hook"))

    async def events(self) -> AsyncIterator[DomainEvent]:
        if self._events_consumed:
            raise RuntimeError("events() supports a single consumer (the account's OMS)")
        self._events_consumed = True
        while (event := await self._events.get()) is not None:
            yield event

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                timeout=self._s.request_timeout_s,
                follow_redirects=False,  # never forward signed headers to another host
                trust_env=self._s.trust_env,
                transport=self._http_transport,
            )
        return self._http

    def _ws_proxy(self) -> bool | None:
        return True if self._s.trust_env else None

    async def _establish(self) -> None:
        client = self._client()
        if self._ap is None:
            self._ap = await resolve_access_point(
                self._s.gateway_url, self._acct(), self._signer, client, allow_insecure=self._s.allow_insecure
            )
            _log.info("exness_access_point", account=self._s.account_id, host=self._ap)
        rest = RestClient(self._ap, self._signer, client, self._limits)
        try:
            self._details = await rest.get(
                "getAccountDetails", f"/v1/configuration/accounts/{self._acct()}/account"
            )
            self._limits.configure(
                await rest.get("getLimits", f"/v1/configuration/accounts/{self._acct()}/limits")
            )
            if not self._symbols:
                await self._load_symbols(rest)
            self._rest = rest
            await self._open_events(self._ap)
            if self._subscribed:
                await self._open_ticks(self._ap)
        except BrokerUnavailable:
            self._ap = None  # re-resolve the Access Point on the next attempt
            raise

    async def _load_symbols(self, rest: RestClient) -> None:
        acct = self._acct()
        names = (
            await rest.get("getAvailableInstrumentList", f"/v1/configuration/accounts/{acct}/instruments")
        ).get("instruments", [])
        wanted = set(self._s.symbols)
        chosen = [
            n
            for n in names
            if not wanted
            or mp.canonical_symbol(n, suffix=self._s.symbol_suffix, aliases=self._s.symbol_aliases) in wanted
        ]
        for name in chosen:
            cond = await rest.get(
                "getInstrumentCondition", f"/v1/configuration/accounts/{acct}/instruments/{name}/conditions"
            )
            try:
                info = mp.instrument_from_wire(
                    cond,
                    suffix=self._s.symbol_suffix,
                    aliases=self._s.symbol_aliases,
                    hedging=self._s.hedging,
                )
            except (KeyError, ValueError) as exc:
                _log.warning("exness_symbol_skipped", instrument=name, error=str(exc))
                continue
            self._symbols[info.instrument.symbol] = info
            self._by_name[info.instrument.broker_symbol] = info
        missing = wanted - set(self._symbols)
        if missing:
            _log.warning("exness_symbols_missing", symbols=sorted(missing))

    async def _open_events(self, ap: str) -> None:
        loop = asyncio.get_running_loop()
        self._snapshot_ready = loop.create_future()
        stream = Stream(
            ap,
            f"/v1/server-events/accounts/{self._acct()}/ws/events",
            self._signer,
            on_message=self._on_event_message,
            on_closed=lambda err: self._on_events_closed(stream, err),
            proxy=self._ws_proxy(),
            subprotocol=self._s.ws_subprotocol,
        )
        await stream.connect()
        self._events_ws = stream
        await stream.send({"id": "sub-transactions", "subscribe": {"event": "transactions"}})
        await stream.send({"id": "sub-account-state", "subscribe": {"event": "account_state"}})
        try:
            await asyncio.wait_for(asyncio.shield(self._snapshot_ready), self._s.snapshot_timeout_s)
        except TimeoutError as exc:
            await stream.close()
            raise BrokerUnavailable("no trading_state_snapshot after subscribing to transactions") from exc
        except BaseException:
            await stream.close()
            raise

    async def _open_ticks(self, ap: str) -> None:
        stream = Stream(
            ap,
            f"/v1/server-events/accounts/{self._acct()}/ws/ticks",
            self._signer,
            on_message=self._on_tick_message,
            on_closed=lambda err: self._on_ticks_closed(stream, err),
            proxy=self._ws_proxy(),
            subprotocol=self._s.ws_subprotocol,
        )
        await stream.connect()
        self._ticks_ws = stream
        names = sorted(self._symbols[s].instrument.broker_symbol for s in self._subscribed)
        await stream.send({"id": "sub-ticks", "subscribe": {"event": "ticks", "instruments": names}})

    def _on_events_closed(self, stream: Stream, error: Exception | None) -> None:
        if stream is not self._events_ws:
            return
        self._events_ws = None
        if self._snapshot_ready is not None and not self._snapshot_ready.done():
            self._snapshot_ready.set_exception(error or BrokerUnavailable("events stream closed"))
            self._snapshot_ready.exception()
        if self._closing:
            return
        if self._connected:
            self._set_connection(ConnectionState.DOWN, str(error) if error else "events stream closed")
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect(), name="exness-reconnect")

    def _on_ticks_closed(self, stream: Stream, error: Exception | None) -> None:
        if stream is not self._ticks_ws or self._closing:
            return
        self._ticks_ws = None
        if self._connected and (self._reconnect_task is None or self._reconnect_task.done()):
            self._events.put_nowait(
                ConnectionEvent(
                    account_id=self._s.account_id,
                    broker=self.name,
                    state=ConnectionState.DEGRADED,
                    ts=self._now(),
                    reason=f"ticks stream lost: {error}",
                )
            )
            self._ticks_task = asyncio.create_task(self._reconnect_ticks(), name="exness-ticks-reconnect")

    def _backoff(self, attempt: int) -> float:
        return min(self._s.reconnect_max_s, self._s.reconnect_initial_s * 2.0**attempt) * self._rng.uniform(
            0.8, 1.2
        )

    async def _reconnect(self) -> None:
        if self._ticks_ws is not None:
            await self._ticks_ws.close()
            self._ticks_ws = None
        attempt = 0
        while not self._closing:
            await asyncio.sleep(self._backoff(attempt))
            try:
                await self._establish()
            except (BrokerError, OSError) as exc:
                attempt += 1
                _log.warning("exness_reconnect_failed", attempt=attempt, error=str(exc))
                continue
            self._set_connection(ConnectionState.UP, f"reconnected after {attempt + 1} attempt(s)")
            return

    async def _reconnect_ticks(self) -> None:
        attempt = 0
        while not self._closing and self._connected and self._ap is not None:
            await asyncio.sleep(self._backoff(attempt))
            try:
                await self._open_ticks(self._ap)
            except BrokerError as exc:
                attempt += 1
                _log.warning("exness_ticks_reconnect_failed", attempt=attempt, error=str(exc))
                continue
            self._events.put_nowait(
                ConnectionEvent(
                    account_id=self._s.account_id,
                    broker=self.name,
                    state=ConnectionState.UP,
                    ts=self._now(),
                    reason="ticks stream restored",
                )
            )
            return

    def _set_connection(self, state: ConnectionState, reason: str) -> None:
        self._connected = state is ConnectionState.UP
        if self._connected:
            self._awaiting_snapshot = True
        self._events.put_nowait(
            ConnectionEvent(
                account_id=self._s.account_id, broker=self.name, state=state, ts=self._now(), reason=reason
            )
        )

    # ------------------------------------------------------------------ queries

    def _require_rest(self) -> RestClient:
        if not self._connected or self._rest is None:
            raise BrokerUnavailable("exness is not connected")
        return self._rest

    def _info(self, symbol: str) -> mp.SymbolInfo:
        info = self._symbols.get(symbol)
        if info is None:
            raise UnknownSymbol(f"{symbol} is not loaded on exness account {self._s.account_id}")
        return info

    async def instruments(self) -> list[Instrument]:
        self._require_rest()
        return [i.instrument for i in self._symbols.values()]

    async def _refresh(self) -> None:
        rest = self._require_rest()
        snap = await rest.get("getTradingStateSnapshot", f"/v1/trading/accounts/{self._acct()}/snapshot")
        self._apply_snapshot(snap)

    def _apply_snapshot(self, snap: Json) -> None:
        self._orders, self._sltp = {}, {}
        for o in snap.get("orders") or []:
            self._cache_order(o)
        self._positions = {
            str(p["position_id"]): p for p in snap.get("positions") or [] if p.get("state") != "closed"
        }
        self._account_state = snap.get("account_state") or self._account_state

    def _cache_order(self, o: Json) -> None:
        oid = str(o["order_id"])
        active = o.get("state") in ("placed", "pending_new")
        if o.get("order_type") == "sltp":
            pid = o.get("position_id")
            if pid is None:
                return  # SL/TP of a pending order: the snapshot shows it on the pending order itself
            if active:
                self._sltp[str(pid)] = o
            elif self._sltp.get(str(pid), {}).get("order_id") == o["order_id"]:
                del self._sltp[str(pid)]
        elif o.get("order_type") in ("limit", "stop") and active:
            self._orders[oid] = o
        else:
            self._orders.pop(oid, None)
            if o.get("order_type") in ("market", "limit", "stop"):
                self._finished[oid] = o
                while len(self._finished) > 5_000:
                    del self._finished[next(iter(self._finished))]

    def _account_view(self) -> AccountState:
        st = self._account_state
        settings = self._details.get("settings") or {}
        balance = mp.dec(st.get("balance", "0"))
        equity = mp.dec(st.get("equity", st.get("balance", "0")))
        margin = mp.dec(st.get("used_margin", "0"))
        lev_raw = str(settings.get("leverage") or st.get("leverage") or "1")
        leverage = (
            int(Decimal(lev_raw.split(":")[-1])) if lev_raw.split(":")[-1].replace(".", "").isdigit() else 1
        )
        return AccountState(
            account_id=self._s.account_id,
            currency=str(settings.get("currency", "")),
            balance=balance,
            equity=equity,
            margin=margin,
            free_margin=equity - margin,
            leverage=max(1, leverage),
            ts=self._now(),
            margin_level=equity / margin * 100 if margin > 0 else None,
        )

    def _position_view(self, p: Json) -> Position | None:
        info = self._by_name.get(p.get("instrument", ""))
        if info is None:
            return None
        pid = str(p["position_id"])
        sltp = self._sltp.get(pid) or {}
        return mp.position_from_wire(
            p,
            info,
            strategy_id=self._pos_strategy.get(pid, mp.MANUAL_STRATEGY),
            sl=mp.opt_dec(sltp.get("stop_loss_price")),
            tp=mp.opt_dec(sltp.get("take_profit_price")),
        )

    def _order_view(self, o: Json, reason: str | None = None) -> Order | None:
        info = self._by_name.get(o.get("instrument", ""))
        if info is None or o.get("order_type") not in ("market", "limit", "stop"):
            return None
        try:
            return mp.order_from_wire(o, info, self._order_tag.get(str(o["order_id"])), reason=reason)
        except (KeyError, ValueError) as exc:
            _log.warning("exness_order_unmappable", order_id=o.get("order_id"), error=str(exc))
            return None

    async def account(self) -> AccountState:
        await self._refresh()
        return self._account_view()

    async def positions(self) -> list[Position]:
        await self._refresh()
        return [v for p in self._positions.values() if (v := self._position_view(p)) is not None]

    async def open_orders(self) -> list[Order]:
        await self._refresh()
        return [v for o in self._orders.values() if (v := self._order_view(o)) is not None]

    async def snapshot(self) -> BrokerSnapshot:
        await self._refresh()
        snap = BrokerSnapshot(
            account=self._account_view(),
            positions=tuple(
                pv for p in self._positions.values() if (pv := self._position_view(p)) is not None
            ),
            orders=tuple(ov for o in self._orders.values() if (ov := self._order_view(o)) is not None),
            ts=self._now(),
        )
        self._awaiting_snapshot = False
        return snap

    async def find_order(self, client_order_id: str, since: datetime) -> Order | None:
        """Exness entities carry no client id, so only orders this adapter instance saw can be found.
        After a restart this returns None; ``idempotent_submit`` makes resubmitting the same key safe."""
        await self._refresh()
        oid = next((o for o, tag in self._order_tag.items() if tag.client_order_id == client_order_id), None)
        if oid is None:
            return None
        wire = self._orders.get(oid) or self._finished.get(oid)
        return self._order_view(wire) if wire is not None else None

    async def limits(self) -> RateLimits:
        return self._limits.published()

    async def subscribe_ticks(self, symbols: Sequence[str]) -> None:
        self._require_rest()
        infos = [self._info(s) for s in symbols]
        new = [i for i in infos if i.instrument.symbol not in self._subscribed]
        self._subscribed.update(i.instrument.symbol for i in infos)
        if not new:
            return
        if self._ticks_ws is not None and self._ticks_ws.is_open:
            names = sorted(i.instrument.broker_symbol for i in new)
            await self._ticks_ws.send(
                {
                    "id": f"sub-ticks-{len(self._subscribed)}",
                    "subscribe": {"event": "ticks", "instruments": names},
                }
            )
        elif self._ap is not None:
            await self._open_ticks(self._ap)

    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]:
        rest = self._require_rest()
        info = self._info(symbol)
        wire_tf = mp.TIMEFRAMES.get(tf)
        if wire_tf is None:
            raise OrderRejected(f"exness does not provide {tf} candles")
        start, end = ensure_utc(start), ensure_utc(end)
        if start >= end:
            raise ValueError("candles: start must be before end")
        duration = tf.duration
        window = duration * self._limits.candles_per_request if duration is not None else end - start
        bars: dict[datetime, Bar] = {}
        cursor, now = start, self._now()
        while cursor < end:
            chunk_end = min(end, cursor + window)
            res = await rest.get(
                "getCandleHistory",
                f"/v1/market-data/accounts/{self._acct()}/candles",
                [
                    ("instrument", info.instrument.broker_symbol),
                    ("timeframe", wire_tf),
                    ("from", mp.iso(cursor)),
                    ("to", mp.iso(chunk_end)),
                    ("price_type", "bid"),
                ],
            )
            for c in res.get("candles") or []:
                bar = mp.bar_from_wire(c, symbol, tf, info.instrument.digits, now)
                if start <= bar.start < end:
                    bars[bar.start] = bar
            cursor = chunk_end
        return [bars[k] for k in sorted(bars)]

    # ------------------------------------------------------------------ trading

    def _comment(self, req: OrderRequest) -> str:
        return f"{req.strategy_id}|{req.strategy_version}"[: self._s.comment_max_len]

    def _price(self, value: Decimal, info: mp.SymbolInfo) -> str:
        return mp.price_str(value, info.instrument.digits)

    async def _send(
        self,
        kind: str,
        op: str,
        method: str,
        path: str,
        *,
        key: str,
        request: OrderRequest | None = None,
        body: Json | None = None,
        params: Sequence[tuple[str, str]] = (),
    ) -> tuple[Json, bool]:
        """Send a mutating request. Returns (ack, duplicate). Registers the pending entry *before* sending
        so a transaction_event that beats the ACK is still correlated."""
        rest = self._require_rest()
        previous = self._pending.get(key)
        pending = previous or _Pending(kind=kind, request=request, sent_mono=time.monotonic())
        self._pending[key] = pending
        ack = await rest.send(op, method, path, idempotency_key=key, body=body, params=params)
        op_id = ack.get("operation_id")
        duplicate = previous is not None and previous.op_id is not None and previous.op_id == op_id
        if op_id is not None:
            pending.op_id = str(op_id)
            pending.sent_mono = time.monotonic() if not duplicate else pending.sent_mono
            self._op_key[str(op_id)] = key
        return ack, duplicate

    async def place_order(self, req: OrderRequest) -> BrokerAck:
        info = self._info(req.symbol)
        inst = info.instrument
        if req.order_type is OrderType.STOP_LIMIT:
            raise OrderRejected("exness has no stop-limit orders")
        if req.time_in_force in (TimeInForce.GTD, TimeInForce.DAY):
            raise OrderRejected("exness pending orders have no expiry; the OMS emulates GTD/DAY")
        if not inst.is_valid_volume(req.volume):
            raise InvalidVolume(f"{req.symbol}: invalid volume {req.volume}")
        cid = req.client_order_id
        if not is_valid_idempotency_key(cid):
            raise OrderRejected("client_order_id is not a valid Exness idempotency key")
        self._tags.setdefault(cid, Tag(cid, req.strategy_id, req.strategy_version))
        body: Json = {
            "instrument": inst.broker_symbol,
            "side": mp.side_to_wire(req.side),
            "volume": mp.volume_str(req.volume),
        }
        if req.sl is not None:
            body["stop_loss_price"] = self._price(req.sl, info)
        if req.tp is not None:
            body["take_profit_price"] = self._price(req.tp, info)
        if comment := self._comment(req):
            body["comment"] = comment
        acct = self._acct()
        if req.order_type is OrderType.MARKET:
            if req.max_slippage is not None and info.exec_mode == "instant":
                ref = self._quotes.get(inst.symbol)
                if ref is None:
                    raise OrderRejected(f"{req.symbol}: no live quote to anchor max_slippage")
                body["price"] = self._price(ref[1] if req.side is Side.BUY else ref[0], info)
                body["deviation"] = str(int(req.max_slippage / inst.tick_size))
            _ack, duplicate = await self._send(
                "open",
                "openPosition",
                "POST",
                f"/v1/trading/accounts/{acct}/positions",
                key=cid,
                request=req,
                body=body,
            )
        else:
            price = req.limit_price if req.order_type is OrderType.LIMIT else req.stop_price
            assert price is not None
            body["type"] = "limit" if req.order_type is OrderType.LIMIT else "stop"
            body["price"] = self._price(price, info)
            _ack, duplicate = await self._send(
                "pending",
                "placePendingOrder",
                "POST",
                f"/v1/trading/accounts/{acct}/orders",
                key=cid,
                request=req,
                body=body,
            )
        known = next((oid for oid, tag in self._order_tag.items() if tag.client_order_id == cid), None)
        if not duplicate:
            now = self._now()
            accepted = replace(
                Order.new(req, now),
                status=OrderStatus.ACCEPTED,
                history=(
                    StatusChange(status=OrderStatus.NEW, ts=now),
                    StatusChange(status=OrderStatus.ACCEPTED, ts=now),
                ),
            )
            self._emit_trade(
                OrderEvent(
                    account_id=self._s.account_id, order=accepted, previous_status=OrderStatus.NEW, ts=now
                )
            )
        return BrokerAck(ts=self._now(), client_order_id=cid, broker_order_id=known, duplicate=duplicate)

    async def modify_order(self, order_id: str, changes: OrderChanges) -> BrokerAck:
        if changes.is_empty:
            raise OrderRejected("no changes requested")
        if changes.volume is not None or changes.expiry is not None:
            raise OrderRejected("exness can modify only price, SL and TP of a pending order")
        o = self._orders.get(order_id)
        if o is None:
            await self._refresh()
            o = self._orders.get(order_id)
        if o is None:
            raise OrderNotFound(order_id)
        info = self._by_name[o["instrument"]]
        body: Json = {}
        new_price = changes.limit_price if changes.limit_price is not None else changes.stop_price
        if new_price is not None:
            body["price"] = self._price(new_price, info)
        if changes.sl is not None or changes.remove_sl:
            body["stop_loss_price"] = None if changes.remove_sl else self._price(changes.sl, info)  # type: ignore[arg-type]
        if changes.tp is not None or changes.remove_tp:
            body["take_profit_price"] = None if changes.remove_tp else self._price(changes.tp, info)  # type: ignore[arg-type]
        key = new_client_order_id()
        await self._send(
            "modify",
            "modifyOrder",
            "PATCH",
            f"/v1/trading/accounts/{self._acct()}/orders/{order_id}",
            key=key,
            body=body,
        )
        tag = self._order_tag.get(order_id)
        return BrokerAck(
            ts=self._now(), client_order_id=tag.client_order_id if tag else None, broker_order_id=order_id
        )

    async def cancel_order(self, order_id: str) -> BrokerAck:
        key = new_client_order_id()
        await self._send(
            "cancel",
            "cancelPendingOrder",
            "DELETE",
            f"/v1/trading/accounts/{self._acct()}/orders/{order_id}",
            key=key,
        )
        return BrokerAck(ts=self._now(), broker_order_id=order_id)

    async def _position(self, position_id: str) -> Json:
        if position_id not in self._positions:
            await self._refresh()
        p = self._positions.get(position_id)
        if p is None or p.get("instrument") not in self._by_name:
            raise PositionNotFound(position_id)
        return p

    async def modify_position(self, position_id: str, sl: Decimal | None, tp: Decimal | None) -> BrokerAck:
        p = await self._position(position_id)
        info = self._by_name[p["instrument"]]
        acct, key = self._acct(), new_client_order_id()
        sltp = self._sltp.get(position_id)
        if sltp is not None and sl is None and tp is None:
            await self._send(
                "sltp",
                "cancelPendingOrder",
                "DELETE",
                f"/v1/trading/accounts/{acct}/orders/{sltp['order_id']}",
                key=key,
            )
        elif sltp is not None:
            body: Json = {
                "stop_loss_price": self._price(sl, info) if sl is not None else None,
                "take_profit_price": self._price(tp, info) if tp is not None else None,
            }
            await self._send(
                "sltp",
                "modifyOrder",
                "PATCH",
                f"/v1/trading/accounts/{acct}/orders/{sltp['order_id']}",
                key=key,
                body=body,
            )
        elif sl is not None or tp is not None:
            body = {"type": "sltp", "position_id": position_id}
            if sl is not None:
                body["stop_loss_price"] = self._price(sl, info)
            if tp is not None:
                body["take_profit_price"] = self._price(tp, info)
            await self._send(
                "sltp", "placePendingOrder", "POST", f"/v1/trading/accounts/{acct}/orders", key=key, body=body
            )
        return BrokerAck(ts=self._now(), position_id=position_id)

    async def close_position(self, position_id: str, volume: Decimal | None = None) -> BrokerAck:
        p = await self._position(position_id)
        open_volume = mp.dec(p["volume"])
        params: list[tuple[str, str]] = []
        if volume is not None:
            if volume > open_volume or volume <= 0:
                raise InvalidVolume(f"cannot close {volume} of {open_volume} lots")
            if volume < open_volume:
                params.append(("volume", mp.volume_str(volume)))
        key = new_client_order_id()
        await self._send(
            "close",
            "closePosition",
            "DELETE",
            f"/v1/trading/accounts/{self._acct()}/positions/{position_id}",
            key=key,
            params=params,
        )
        return BrokerAck(ts=self._now(), position_id=position_id)

    # ------------------------------------------------------------------ inbound

    def _emit_trade(self, event: DomainEvent) -> None:
        if self._connected and not self._awaiting_snapshot:
            self._events.put_nowait(event)

    def _on_tick_message(self, msg: Json) -> None:
        if "code" in msg and "error_message" in msg:
            _log.warning("exness_ticks_error", id=msg.get("id"), error=msg.get("error_message"))
            return
        info = self._by_name.get(msg.get("instrument", ""))
        if info is None or "bid" not in msg or "ask" not in msg:
            return
        tick = mp.tick_from_wire(
            msg, info.instrument.symbol, info.instrument.digits, now=self._now(), mono_ns=time.monotonic_ns()
        )
        self._quotes[info.instrument.symbol] = (tick.bid, tick.ask)
        if self._connected and info.instrument.symbol in self._subscribed:
            self._events.put_nowait(TickEvent(tick=tick, broker=self.name))

    def _on_event_message(self, msg: Json) -> None:
        kind = msg.get("event_type")
        if kind == "trading_state_snapshot":
            self._apply_snapshot(msg.get("payload") or {})
            if self._snapshot_ready is not None and not self._snapshot_ready.done():
                self._snapshot_ready.set_result(None)
        elif kind == "transaction_event":
            self._on_transaction(msg)
        elif kind == "account_state_event":
            self._account_state = (msg.get("payload") or {}).get("account_state") or self._account_state
            account = self._account_view()
            self._emit_trade(AccountEvent(account_id=self._s.account_id, state=account, ts=account.ts))
        elif "code" in msg and "error_message" in msg:
            _log.warning("exness_events_error", id=msg.get("id"), error=msg.get("error_message"))
            if (
                self._snapshot_ready is not None
                and not self._snapshot_ready.done()
                and msg.get("id") == "sub-transactions"
            ):
                self._snapshot_ready.set_exception(mp.map_error(str(msg["error_message"])))
                self._snapshot_ready.exception()

    def _on_transaction(self, msg: Json) -> None:
        now = self._now()
        key = msg.get("client_request_id") or self._op_key.get(str(msg.get("operation_id")), "")
        pending = self._pending.get(key) if key else None
        if pending is not None:
            pending.done = True
        if msg.get("status") != "success":
            self._on_rejected(pending, str(msg.get("error_message") or "REJECTED"), now)
            return
        payload: Json = msg.get("payload") or {}
        ours = pending is not None and pending.kind in _OPEN_KINDS
        tag = self._tags.get(key) if ours else None
        deals: list[Json] = payload.get("deals") or []
        closing_orders = {str(d["order_id"]) for d in deals if d.get("type") == "close" and d.get("order_id")}
        orders: list[Json] = payload.get("orders") or []
        sltp_touched: set[str] = set()

        for o in orders:
            oid = str(o["order_id"])
            if o.get("order_type") == "sltp":
                if o.get("position_id") is not None:
                    sltp_touched.add(str(o["position_id"]))
                self._cache_order(o)
                continue
            internal = (
                (pending is not None and pending.kind == "close")
                or oid in closing_orders
                or (o.get("reason") in ("sl", "tp", "so", "close_by"))
            )
            if tag is not None and not internal:
                self._order_tag.setdefault(oid, tag)
            self._cache_order(o)
            if internal:
                continue
            order_view = self._order_view(o)
            if order_view is not None:
                prev = (
                    OrderStatus.ACCEPTED if order_view.status is not OrderStatus.ACCEPTED else OrderStatus.NEW
                )
                self._emit_trade(
                    OrderEvent(account_id=self._s.account_id, order=order_view, previous_status=prev, ts=now)
                )

        for d in deals:
            if d.get("type") not in ("open", "close"):
                continue
            fill = self._fill_from_deal(d, orders, tag)
            if fill is not None:
                self._emit_trade(FillEvent(account_id=self._s.account_id, fill=fill, ts=now))

        for p in payload.get("positions") or []:
            self._apply_position(p, tag, now)
        for pid in sltp_touched - {str(p["position_id"]) for p in payload.get("positions") or []}:
            cached = self._positions.get(pid)
            view = self._position_view(cached) if cached else None
            if view is not None:
                self._emit_trade(
                    PositionEvent(
                        account_id=self._s.account_id, position=view, change=PositionChange.MODIFIED, ts=now
                    )
                )

        if payload.get("account_state"):
            self._account_state = payload["account_state"]
            account = self._account_view()
            self._emit_trade(AccountEvent(account_id=self._s.account_id, state=account, ts=account.ts))

    def _on_rejected(self, pending: _Pending | None, reason: str, now: datetime) -> None:
        if pending is None or pending.request is None or pending.kind not in _OPEN_KINDS:
            _log.warning("exness_operation_rejected", kind=pending.kind if pending else None, reason=reason)
            return
        req = pending.request
        rejected = Order(
            request=req,
            status=OrderStatus.REJECTED,
            history=(
                StatusChange(status=OrderStatus.NEW, ts=now),
                StatusChange(status=OrderStatus.ACCEPTED, ts=now),
                StatusChange(status=OrderStatus.REJECTED, ts=now, reason=reason),
            ),
            rejection_reason=reason,
        )
        self._emit_trade(
            OrderEvent(
                account_id=self._s.account_id, order=rejected, previous_status=OrderStatus.ACCEPTED, ts=now
            )
        )

    def _fill_from_deal(self, d: Json, orders: list[Json], tag: Tag | None) -> Fill | None:
        info = self._by_name.get(d.get("instrument", ""))
        if info is None or not d.get("volume") or not d.get("price"):
            return None
        order_id = str(d.get("order_id") or "")
        if (
            not order_id
        ):  # the deal may omit order_id: take the order of the same position in this transaction
            order_id = next(
                (
                    str(o["order_id"])
                    for o in orders
                    if o.get("position_id") == d.get("position_id") and o.get("order_type") != "sltp"
                ),
                "",
            )
        is_open = d.get("type") == "open"
        owner = self._order_tag.get(order_id) or (tag if is_open else None)
        profit = (d.get("profit") or {}).get("amount")
        return Fill(
            fill_id=str(d["deal_id"]),
            client_order_id=owner.client_order_id if owner and is_open else None,
            broker_order_id=order_id,
            position_id=str(d.get("position_id") or ""),
            symbol=info.instrument.symbol,
            side=mp.side_from_wire(d["direction"]),
            volume=mp.dec(d["volume"]),
            price=mp.dec(d["price"]).quantize(info.instrument.tick_size),
            ts=mp.parse_time(d["create_time"]),
            commission=mp.opt_dec(d.get("commission")) or ZERO,
            realized_pnl=mp.dec(profit) if profit is not None and not is_open else None,
        )

    def _apply_position(self, p: Json, tag: Tag | None, now: datetime) -> None:
        pid = str(p["position_id"])
        previous = self._positions.get(pid)
        if p.get("state") == "closed" or mp.dec(p.get("volume", "0")) == 0:
            self._positions.pop(pid, None)
            self._sltp.pop(pid, None)
            base = previous or p
            view = self._position_view(
                {
                    **base,
                    "volume": base.get("volume")
                    if mp.dec(base.get("volume", "0")) > 0
                    else base.get("initial_volume", "0"),
                }
            )
            if previous is not None and view is not None:
                self._emit_trade(
                    PositionEvent(
                        account_id=self._s.account_id, position=view, change=PositionChange.CLOSED, ts=now
                    )
                )
            return
        if previous is None and tag is not None:
            self._pos_strategy.setdefault(pid, tag.strategy_id)
        self._positions[pid] = p
        if previous is None:
            change = PositionChange.OPENED
        elif mp.dec(p["volume"]) < mp.dec(previous["volume"]):
            change = PositionChange.PARTIALLY_CLOSED
        else:
            change = PositionChange.MODIFIED
        view = self._position_view(p)
        if view is not None:
            self._emit_trade(
                PositionEvent(account_id=self._s.account_id, position=view, change=change, ts=now)
            )

    # ------------------------------------------------------------------ recovery

    async def _watch_operations(self) -> None:
        """If an ACKed operation's transaction_event does not arrive in time, poll its status; if it was
        confirmed (the event was missed), resubscribe so the fresh snapshot becomes the baseline."""
        while True:
            await asyncio.sleep(1.0)
            if not self._connected or self._rest is None:
                continue
            deadline = time.monotonic() - self._s.operation_timeout_s
            stale = [
                (k, p) for k, p in self._pending.items() if not p.done and p.op_id and p.sent_mono < deadline
            ]
            for key, pending in stale:
                try:
                    status = await self._rest.get(
                        "getOperationStatus",
                        f"/v1/trading/accounts/{self._acct()}/operations/{pending.op_id}",
                    )
                except OrderNotFound:
                    pending.done = True
                    continue
                except BrokerError as exc:
                    if exc.broker_code == "REQUEST_OPERATION_NOT_FOUND":
                        pending.done = True
                    continue
                state = status.get("status")
                if state == "pending":
                    pending.sent_mono = time.monotonic()
                    continue
                pending.done = True
                if state in ("rejected", "failed"):
                    self._on_rejected(pending, str(status.get("error_message") or state), self._now())
                else:
                    _log.warning("exness_event_missed", operation_id=pending.op_id, key=key)
                    with contextlib.suppress(BrokerError, KeyError, ValueError):
                        await self._recover_operation(key, pending, status)
                    self._spawn(self.drop_connection())
            # forget finished requests after a while (keep keys long enough to answer duplicate submits)
            cutoff = time.monotonic() - 3600
            for key in [k for k, p in self._pending.items() if p.done and p.sent_mono < cutoff]:
                self._pending.pop(key, None)

    async def _recover_operation(self, key: str, pending: _Pending, status: Json) -> None:
        """A confirmed operation whose transaction_event we never saw, e.g. a resend after a restart that the
        server de-duplicated. Entities carry no client id, so re-link it by content within the operation's
        lifetime: a market open via its ``open`` deal (closing orders look identical in order history), a
        pending order via the snapshot (still working) or order history (filled/cancelled). Exactly one
        untagged match is required; anything else is left alone and the OMS treats the order as not found."""
        req = pending.request
        rest = self._rest
        info = self._symbols.get(req.symbol) if req is not None else None
        if req is None or info is None or rest is None or pending.kind not in _OPEN_KINDS:
            return
        since = mp.parse_time(str(status["created_at"])) - timedelta(seconds=10)
        side = mp.side_to_wire(req.side)

        def same(o: Json) -> bool:
            return (
                o.get("instrument") == info.instrument.broker_symbol
                and o.get("direction") == side
                and o.get("volume") is not None
                and mp.dec(o["volume"]) == req.volume
                and str(o.get("order_id")) not in self._order_tag
            )

        history = await self._history(rest, "orders", since, info.instrument.broker_symbol)
        if pending.kind == "open":
            deals = await self._history(rest, "deals", since, info.instrument.broker_symbol, deal_type="open")
            opened = {str(d["order_id"]) for d in deals if d.get("order_id") and same(d)}
            matches = [o for o in history if o.get("order_type") == "market" and str(o["order_id"]) in opened]
        else:
            await self._refresh()
            wanted = req.order_type.value.lower()
            price = req.limit_price if req.order_type is OrderType.LIMIT else req.stop_price
            matches = [
                o
                for o in [*self._orders.values(), *history]
                if o.get("order_type") == wanted
                and same(o)
                and (price is None or (o.get("price") is not None and mp.dec(o["price"]) == price))
            ]
        if len(matches) != 1:
            _log.warning("exness_recover_ambiguous", key=key, matches=len(matches))
            return
        order = matches[0]
        oid = str(order["order_id"])
        tag = self._tags.get(key) or Tag(key, req.strategy_id, req.strategy_version)
        self._order_tag[oid] = tag
        if order.get("state") not in ("placed", "pending_new"):
            self._finished[oid] = order
        if order.get("position_id"):
            self._pos_strategy.setdefault(str(order["position_id"]), tag.strategy_id)
        _log.info("exness_operation_recovered", key=key, order_id=oid)

    async def _history(
        self,
        rest: RestClient,
        kind: str,
        since: datetime,
        instrument: str | None = None,
        *,
        until: datetime | None = None,
        **filters: str,
    ) -> list[Json]:
        """``GET /v1/history/accounts/{id}/{orders|deals}`` for [since, until) (until = now if omitted),
        following cursors (a cursor cannot be combined with filters, only ``limit``). Returns the bare
        order/deal objects. Never truncates: past the page budget it raises."""
        op = "getOrdersHistory" if kind == "orders" else "getDealsHistory"
        path = f"/v1/history/accounts/{self._acct()}/{kind}"
        item_key = kind[:-1]
        params: list[tuple[str, str]] = [("from", mp.iso(since)), ("limit", "200")]
        if until is not None:
            params.append(("to", mp.iso(until)))
        if instrument is not None:
            params.append(("instrument", instrument))
        params.extend(filters.items())
        out: list[Json] = []
        for _ in range(_HISTORY_MAX_PAGES):
            res = await rest.get(op, path, params)
            out.extend(i[item_key] for i in res.get("items") or [] if isinstance(i.get(item_key), dict))
            cursor = res.get("next_cursor")
            if not res.get("has_more") or not cursor:
                return out
            params = [("cursor", str(cursor)), ("limit", "200")]
        raise BrokerUnavailable(
            f"exness {kind} history: more than {_HISTORY_MAX_PAGES} pages; narrow the window"
        )

    async def deal_history(self, start: datetime, end: datetime) -> list[Fill]:
        """Trade deals (``open``/``close``) executed in [start, end), oldest first. Balance-type deals
        (deposits, fees, swaps charged as balance operations) are not fills and are skipped."""
        rest = self._require_rest()
        fills = []
        for d in await self._history(rest, "deals", start, until=end):
            if d.get("type") not in ("open", "close"):
                continue
            fill = self._fill_from_deal(d, [], None)
            if fill is not None and start <= fill.ts < end:
                fills.append(fill)
        return sorted(fills, key=lambda f: (f.ts, f.fill_id))

    def _spawn(self, coro: Coroutine[None, None, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
