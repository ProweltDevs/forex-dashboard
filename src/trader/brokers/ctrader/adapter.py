"""cTrader Open API adapter (Protobuf over TLS). See docs/brokers/ctrader.md for verified protocol facts
and docs/adr/0004-ctrader-adapter.md for design decisions.

Connection lifecycle: TLS connect → ProtoOAApplicationAuthReq → (ProtoOARefreshTokenReq if the access
token is near expiry) → ProtoOAAccountAuthReq → trader/assets/symbols → resubscribe spots. On any drop
the adapter emits ConnectionEvent(DOWN), reconnects with exponential backoff + jitter, emits UP, and
then emits no trade-related events until the OMS has called ``snapshot()``.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import ssl
import time
from collections.abc import AsyncIterator, Coroutine, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

from google.protobuf.message import Message
from pydantic import SecretStr

from trader.brokers.base import BrokerAck, Capabilities, OrderChanges, RateLimits
from trader.domain.enums import ConnectionState, OrderStatus, OrderType, PositionChange, Side, Timeframe
from trader.domain.errors import (
    BrokerError,
    BrokerUnavailable,
    InvalidPrice,
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
    Tick,
)
from trader.domain.money import ZERO
from trader.domain.time import Clock, ensure_utc, from_epoch_ms, to_epoch_ms
from trader.observability.logging import get_logger

from . import mapping as mp
from .oauth import TokenSet, TokenStore
from .proto import OpenApiMessages_pb2 as msgs
from .proto import OpenApiModelMessages_pb2 as model
from .transport import LIMIT_HISTORICAL, LIMIT_REQUESTS, Transport

_log = get_logger("trader.brokers.ctrader")

HOSTS = {"demo": "demo.ctraderapi.com", "live": "live.ctraderapi.com"}
PROTOBUF_PORT = 5035
_MAX_BARS = 1000  # bars per trendbar request we aim for; the server limit is undocumented (see probe tests)
_SYMBOL_BATCH = 100


@dataclass(frozen=True, slots=True, kw_only=True)
class CTraderSettings:
    account_id: str  # our config id
    ctid_trader_account_id: int
    environment: Literal["demo", "live"]
    client_id: str
    client_secret: SecretStr
    symbols: tuple[str, ...] = ()  # canonical symbols to load; empty = every enabled symbol
    symbol_aliases: Mapping[str, str] = field(default_factory=dict)  # broker name -> canonical
    host: str | None = None  # override (tests); default from environment
    port: int = PROTOBUF_PORT
    use_tls: bool = True
    proxy: str | None = None  # http://host:port CONNECT proxy, if the network requires one
    ca_file: str | None = None
    request_timeout_s: float = 10.0
    token_refresh_margin: timedelta = timedelta(days=7)
    account_poll_s: float = 5.0
    reconnect_initial_s: float = 0.5
    reconnect_max_s: float = 30.0
    heartbeat_s: float = 10.0
    idle_timeout_s: float = 30.0

    @property
    def resolved_host(self) -> str:
        return self.host or HOSTS[self.environment]


class CTraderBroker:
    def __init__(
        self,
        settings: CTraderSettings,
        *,
        tokens: TokenSet,
        store: TokenStore,
        clock: Clock,
        rng: random.Random | None = None,
    ) -> None:
        self._s = settings
        self._tokens = tokens
        self._store = store
        self._clock = clock
        self._rng = rng or random.Random()
        self._transport: Transport | None = None
        self._connected = False
        self._closing = False
        self._awaiting_snapshot = True
        self._events: asyncio.Queue[DomainEvent | None] = asyncio.Queue()
        self._events_consumed = False
        self._tasks: set[asyncio.Task[None]] = set()
        self._reconnect_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None

        self._trader: model.ProtoOATrader | None = None
        self._assets: dict[int, str] = {}
        self._symbols: dict[str, mp.SymbolInfo] = {}
        self._by_id: dict[int, mp.SymbolInfo] = {}
        self._subscribed: set[str] = set()
        self._quotes: dict[int, tuple[int | None, int | None]] = {}
        self._positions: dict[int, model.ProtoOAPosition] = {}
        self._orders: dict[int, model.ProtoOAOrder] = {}
        self._cid_to_order: dict[str, str] = {}
        self._inflight: dict[str, asyncio.Future[BrokerAck]] = {}

    # ------------------------------------------------------------------ identity

    @property
    def name(self) -> str:
        return "ctrader"

    @property
    def capabilities(self) -> Capabilities:
        netted = self._trader is not None and self._trader.accountType == model.NETTED
        return Capabilities(
            order_types=frozenset(OrderType),
            native_sl_tp=True,
            native_trailing_stop=False,  # cTrader has it, but OrderRequest cannot express it yet
            partial_close=True,
            close_by=False,
            hedging=not netted,
            netting=netted,
            modify_pending=True,
            server_side_expiry=True,
            tick_stream=True,
            # Depth is not documented; these are conservative assumptions checked by the demo probe tests.
            candle_history={tf: timedelta(days=30) for tf in mp.TIMEFRAMES if tf.duration is not None},
            max_candles_per_request=_MAX_BARS,
            sync_order_result=False,
        )

    # ------------------------------------------------------------------ lifecycle

    async def connect(self) -> None:
        if self._connected:
            return
        self._closing = False
        await self._establish()
        self._set_connection(ConnectionState.UP, "connected")
        if self._poll_task is None:
            self._poll_task = asyncio.create_task(self._poll_account(), name="ctrader-account-poll")

    async def close(self) -> None:
        self._closing = True
        for task in [t for t in (self._reconnect_task, self._poll_task, *self._tasks) if t is not None]:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._reconnect_task = self._poll_task = None
        if self._transport is not None:
            with contextlib.suppress(Exception):
                await self._transport.request(
                    msgs.ProtoOAAccountLogoutReq(ctidTraderAccountId=self._s.ctid_trader_account_id),
                    msgs.ProtoOAAccountLogoutRes,
                    timeout=2,
                )
            await self._transport.close()
            self._transport = None
        if self._connected:
            self._set_connection(ConnectionState.DOWN, "closed")
        self._events.put_nowait(None)

    async def drop_connection(self) -> None:
        """Test hook: kill the socket as a network failure would; the adapter reconnects by itself."""
        if self._transport is not None:
            await self._transport.close()

    async def events(self) -> AsyncIterator[DomainEvent]:
        if self._events_consumed:
            raise RuntimeError("events() supports a single consumer (the account's OMS)")
        self._events_consumed = True
        while (event := await self._events.get()) is not None:
            yield event

    def _ssl_context(self) -> ssl.SSLContext | None:
        if not self._s.use_tls:
            return None
        return ssl.create_default_context(cafile=self._s.ca_file)

    async def _establish(self) -> None:
        transport = Transport(
            self._s.resolved_host,
            self._s.port,
            on_message=self._on_message,
            on_closed=self._on_closed,
            ssl_context=self._ssl_context(),
            proxy=self._s.proxy,
            request_timeout=self._s.request_timeout_s,
            heartbeat_s=self._s.heartbeat_s,
            idle_timeout=self._s.idle_timeout_s,
        )
        await transport.connect()
        try:
            await transport.request(
                msgs.ProtoOAApplicationAuthReq(
                    clientId=self._s.client_id, clientSecret=self._s.client_secret.get_secret_value()
                ),
                msgs.ProtoOAApplicationAuthRes,
            )
            if self._tokens.expires_within(self._clock.now(), self._s.token_refresh_margin):
                await self._refresh_tokens(transport)
            await transport.request(
                msgs.ProtoOAAccountAuthReq(
                    ctidTraderAccountId=self._s.ctid_trader_account_id,
                    accessToken=self._tokens.access_token.get_secret_value(),
                ),
                msgs.ProtoOAAccountAuthRes,
            )
            ctid = self._s.ctid_trader_account_id
            self._trader = (
                await transport.request(
                    msgs.ProtoOATraderReq(ctidTraderAccountId=ctid), msgs.ProtoOATraderRes
                )
            ).trader
            if not self._symbols:
                await self._load_symbols(transport)
            if self._subscribed:
                ids = [self._symbols[s].symbol_id for s in self._subscribed]
                await self._subscribe(transport, ids)
        except BaseException:
            await transport.close()
            raise
        self._transport = transport

    async def _refresh_tokens(self, transport: Transport) -> None:
        res = await transport.request(
            msgs.ProtoOARefreshTokenReq(refreshToken=self._tokens.refresh_token.get_secret_value()),
            msgs.ProtoOARefreshTokenRes,
        )
        self._tokens = TokenSet.from_response(
            res.accessToken, res.refreshToken, res.expiresIn, self._clock.now()
        )
        if not self._store.persistent:
            _log.warning(
                "ctrader_tokens_not_persisted", detail="set TRADER_SECRETS_KEY to keep refreshed tokens"
            )
        else:
            self._store.save(self._tokens)
        _log.info("ctrader_token_refreshed", expires_at=self._tokens.expires_at)

    async def _load_symbols(self, transport: Transport) -> None:
        ctid = self._s.ctid_trader_account_id
        assets = await transport.request(
            msgs.ProtoOAAssetListReq(ctidTraderAccountId=ctid), msgs.ProtoOAAssetListRes
        )
        self._assets = {a.assetId: a.name for a in assets.asset}
        listing = await transport.request(
            msgs.ProtoOASymbolsListReq(ctidTraderAccountId=ctid), msgs.ProtoOASymbolsListRes
        )
        wanted = set(self._s.symbols)
        lights = {
            light.symbolId: light
            for light in listing.symbol
            if light.enabled
            and (not wanted or mp.canonical_symbol(light.symbolName, self._s.symbol_aliases) in wanted)
        }
        ids = list(lights)
        hedging = self._trader is None or self._trader.accountType != model.NETTED
        now = self._clock.now()
        for i in range(0, len(ids), _SYMBOL_BATCH):
            batch = await transport.request(
                msgs.ProtoOASymbolByIdReq(ctidTraderAccountId=ctid, symbolId=ids[i : i + _SYMBOL_BATCH]),
                msgs.ProtoOASymbolByIdRes,
            )
            for full in batch.symbol:
                try:
                    info = mp.instrument_from_wire(
                        lights[full.symbolId], full, self._assets,
                        aliases=self._s.symbol_aliases, hedging=hedging, reference=now,
                    )  # fmt: skip
                except (ValueError, KeyError) as exc:
                    _log.warning("ctrader_symbol_skipped", symbol_id=full.symbolId, error=str(exc))
                    continue
                self._symbols[info.instrument.symbol] = info
                self._by_id[info.symbol_id] = info
        missing = wanted - set(self._symbols)
        if missing:
            _log.warning("ctrader_symbols_missing", symbols=sorted(missing))

    def _on_closed(self, error: Exception | None) -> None:
        self._transport = None
        if self._closing:
            return
        was_up = self._connected
        if was_up:
            self._set_connection(ConnectionState.DOWN, str(error) if error else "connection closed")
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect(), name="ctrader-reconnect")

    async def _reconnect(self) -> None:
        attempt = 0
        while not self._closing:
            delay = min(self._s.reconnect_max_s, self._s.reconnect_initial_s * 2**attempt)
            await asyncio.sleep(delay * self._rng.uniform(0.8, 1.2))
            try:
                await self._establish()
            except (BrokerError, OSError, TimeoutError) as exc:
                attempt += 1
                _log.warning("ctrader_reconnect_failed", attempt=attempt, error=str(exc))
                continue
            self._set_connection(ConnectionState.UP, f"reconnected after {attempt + 1} attempt(s)")
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

    # ------------------------------------------------------------------ requests

    def _now(self) -> datetime:
        return self._clock.now()

    def _ctid(self) -> int:
        return self._s.ctid_trader_account_id

    async def _req[M: Message](self, message: Message, response: type[M], *, historical: bool = False) -> M:
        transport = self._transport
        if not self._connected or transport is None:
            raise BrokerUnavailable("ctrader is not connected")
        return await transport.request(message, response, historical=historical)

    def _info(self, symbol: str) -> mp.SymbolInfo:
        info = self._symbols.get(symbol)
        if info is None:
            raise UnknownSymbol(f"{symbol} is not loaded on ctrader account {self._s.account_id}")
        return info

    async def instruments(self) -> list[Instrument]:
        if not self._connected:
            raise BrokerUnavailable("ctrader is not connected")
        return [i.instrument for i in self._symbols.values()]

    async def _reconcile(self) -> msgs.ProtoOAReconcileRes:
        res = await self._req(
            msgs.ProtoOAReconcileReq(ctidTraderAccountId=self._ctid()), msgs.ProtoOAReconcileRes
        )
        self._positions = {p.positionId: p for p in res.position}
        self._orders = {o.orderId: o for o in res.order if not mp.is_internal_order(o)}
        return res

    async def _unrealized(self) -> dict[int, Decimal]:
        res = await self._req(
            msgs.ProtoOAGetPositionUnrealizedPnLReq(ctidTraderAccountId=self._ctid()),
            msgs.ProtoOAGetPositionUnrealizedPnLRes,
        )
        return {
            p.positionId: mp.money(p.netUnrealizedPnL, res.moneyDigits) for p in res.positionUnrealizedPnL
        }

    def _account_from(
        self,
        trader: model.ProtoOATrader,
        positions: Sequence[model.ProtoOAPosition],
        pnl: Mapping[int, Decimal],
    ) -> AccountState:
        digits = trader.moneyDigits
        balance = mp.money(trader.balance, digits)
        equity = balance + sum(pnl.values(), ZERO)
        margin = sum((mp.money(p.usedMargin, p.moneyDigits or digits) for p in positions), ZERO)
        return AccountState(
            account_id=self._s.account_id,
            currency=self._assets.get(trader.depositAssetId, ""),
            balance=balance,
            equity=equity,
            margin=margin,
            free_margin=equity - margin,
            leverage=max(1, trader.leverageInCents // 100),
            ts=self._now(),
            margin_level=equity / margin * 100 if margin > 0 else None,
        )

    async def _fetch_state(self) -> tuple[AccountState, list[Position], list[Order]]:
        trader = (
            await self._req(msgs.ProtoOATraderReq(ctidTraderAccountId=self._ctid()), msgs.ProtoOATraderRes)
        ).trader
        self._trader = trader
        rec = await self._reconcile()
        pnl = await self._unrealized() if rec.position else {}
        account = self._account_from(trader, rec.position, pnl)
        positions = [
            mp.position_from_wire(
                p, self._by_id[p.tradeData.symbolId], unrealized=pnl.get(p.positionId, ZERO)
            )
            for p in rec.position
            if p.tradeData.symbolId in self._by_id
        ]
        orders = [
            mp.order_from_wire(o, self._by_id[o.tradeData.symbolId])
            for o in rec.order
            if o.tradeData.symbolId in self._by_id and not mp.is_internal_order(o)
        ]
        return account, positions, orders

    async def account(self) -> AccountState:
        return (await self._fetch_state())[0]

    async def positions(self) -> list[Position]:
        return (await self._fetch_state())[1]

    async def open_orders(self) -> list[Order]:
        return (await self._fetch_state())[2]

    async def snapshot(self) -> BrokerSnapshot:
        account, positions, orders = await self._fetch_state()
        self._awaiting_snapshot = False
        return BrokerSnapshot(
            account=account, positions=tuple(positions), orders=tuple(orders), ts=self._now()
        )

    async def limits(self) -> RateLimits:
        return RateLimits(buckets=(LIMIT_REQUESTS, LIMIT_HISTORICAL))

    async def subscribe_ticks(self, symbols: Sequence[str]) -> None:
        infos = [self._info(s) for s in symbols]
        transport = self._transport
        if not self._connected or transport is None:
            raise BrokerUnavailable("ctrader is not connected")
        new = [i for i in infos if i.instrument.symbol not in self._subscribed]
        if new:
            await self._subscribe(transport, [i.symbol_id for i in new])
        self._subscribed.update(i.instrument.symbol for i in infos)

    async def _subscribe(self, transport: Transport, symbol_ids: list[int]) -> None:
        try:
            await transport.request(
                msgs.ProtoOASubscribeSpotsReq(
                    ctidTraderAccountId=self._ctid(), symbolId=symbol_ids, subscribeToSpotTimestamp=True
                ),
                msgs.ProtoOASubscribeSpotsRes,
            )
        except BrokerError as exc:
            if exc.broker_code != "ALREADY_SUBSCRIBED":
                raise

    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]:
        info = self._info(symbol)
        period = mp.TIMEFRAMES.get(tf)
        duration = tf.duration
        if period is None or duration is None:
            raise OrderRejected(f"ctrader does not provide {tf} candles")
        start, end = ensure_utc(start), ensure_utc(end)
        if start >= end:
            raise ValueError("candles: start must be before end")
        bars: dict[datetime, Bar] = {}
        window = duration * _MAX_BARS
        cursor = start
        now = self._now()
        while cursor < end:
            chunk_end = min(end, cursor + window)
            try:
                res = await self._req(
                    msgs.ProtoOAGetTrendbarsReq(
                        ctidTraderAccountId=self._ctid(),
                        symbolId=info.symbol_id,
                        period=period,
                        fromTimestamp=to_epoch_ms(cursor),
                        toTimestamp=to_epoch_ms(chunk_end),
                    ),
                    msgs.ProtoOAGetTrendbarsRes,
                    historical=True,
                )
            except BrokerError as exc:
                if exc.broker_code == "INCORRECT_BOUNDARIES" and window > duration:
                    window = max(duration, window / 2)  # undocumented per-period limit: shrink and retry
                    continue
                raise
            for tb in res.trendbar:
                bar = mp.bar_from_wire(tb, symbol, tf, info.instrument.digits, now)
                if start <= bar.start < end:
                    bars[bar.start] = bar
            cursor = chunk_end
        return [bars[k] for k in sorted(bars)]

    # ------------------------------------------------------------------ trading

    async def place_order(self, req: OrderRequest) -> BrokerAck:
        cid = req.client_order_id
        if (existing := self._cid_to_order.get(cid)) is not None:
            return BrokerAck(ts=self._now(), client_order_id=cid, broker_order_id=existing, duplicate=True)
        if (inflight := self._inflight.get(cid)) is not None:
            first = await asyncio.shield(inflight)
            return replace(first, duplicate=True)
        if len(cid) > 50:
            raise OrderRejected("cTrader clientOrderId is limited to 50 characters")
        info = self._info(req.symbol)
        inst = info.instrument
        if not inst.is_valid_volume(req.volume):
            raise InvalidVolume(f"{req.symbol}: invalid volume {req.volume}")
        message = self._new_order_message(req, info)

        future: asyncio.Future[BrokerAck] = asyncio.get_running_loop().create_future()
        self._inflight[cid] = future
        try:
            ev = await self._req(message, msgs.ProtoOAExecutionEvent)
            order_id = str(ev.order.orderId) if ev.HasField("order") else None
            if order_id is not None:
                self._cid_to_order[cid] = order_id
            ack = BrokerAck(
                ts=self._now(),
                client_order_id=cid,
                broker_order_id=order_id,
                position_id=str(ev.position.positionId) if ev.HasField("position") else None,
            )
            future.set_result(ack)
            return ack
        except BaseException as exc:
            future.set_exception(exc)
            future.exception()  # mark retrieved
            raise
        finally:
            self._inflight.pop(cid, None)

    def _new_order_message(self, req: OrderRequest, info: mp.SymbolInfo) -> msgs.ProtoOANewOrderReq:
        inst = info.instrument
        digits = inst.digits
        msg = msgs.ProtoOANewOrderReq(
            ctidTraderAccountId=self._ctid(),
            symbolId=info.symbol_id,
            tradeSide=mp.side_to_wire(req.side),
            volume=mp.lots_to_wire(req.volume, info.lot_size),
            clientOrderId=req.client_order_id,
            label=mp.make_label(req.strategy_id, req.strategy_version),
        )
        if req.order_type is OrderType.MARKET:
            ref = self._reference_price(info, req.side)
            if req.max_slippage is not None:
                msg.orderType = model.MARKET_RANGE
                msg.baseSlippagePrice = mp.price_to_double(ref, digits)
                msg.slippageInPoints = int(req.max_slippage / inst.tick_size)
            else:
                msg.orderType = model.MARKET
            # Absolute SL/TP are not supported on MARKET orders: send them relative to the reference price.
            if req.sl is not None:
                distance = (ref - req.sl) if req.side is Side.BUY else (req.sl - ref)
                if distance <= 0:
                    raise InvalidPrice(f"SL {req.sl} is on the wrong side of {ref} for {req.side}")
                msg.relativeStopLoss = mp.relative_to_int(distance)
            if req.tp is not None:
                distance = (req.tp - ref) if req.side is Side.BUY else (ref - req.tp)
                if distance <= 0:
                    raise InvalidPrice(f"TP {req.tp} is on the wrong side of {ref} for {req.side}")
                msg.relativeTakeProfit = mp.relative_to_int(distance)
            if req.time_in_force.value in ("IOC", "FOK"):
                msg.timeInForce = mp.tif_to_wire(req.time_in_force)
            return msg

        msg.orderType = mp.order_type_to_wire(req.order_type)
        msg.timeInForce = mp.tif_to_wire(req.time_in_force)
        if req.expiry is not None:
            msg.expirationTimestamp = to_epoch_ms(req.expiry)
        if req.order_type is OrderType.LIMIT:
            assert req.limit_price is not None
            msg.limitPrice = mp.price_to_double(req.limit_price, digits)
        else:
            assert req.stop_price is not None
            msg.stopPrice = mp.price_to_double(req.stop_price, digits)
        if req.order_type is OrderType.STOP_LIMIT:
            assert req.limit_price is not None
            assert req.stop_price is not None
            # cTrader STOP_LIMIT = stop trigger + max slippage in points beyond it.
            msg.slippageInPoints = int(abs(req.limit_price - req.stop_price) / inst.tick_size)
        if req.sl is not None:
            msg.stopLoss = mp.price_to_double(req.sl, digits)
        if req.tp is not None:
            msg.takeProfit = mp.price_to_double(req.tp, digits)
        return msg

    def _reference_price(self, info: mp.SymbolInfo, side: Side) -> Decimal:
        bid, ask = self._quotes.get(info.symbol_id, (None, None))
        value = ask if side is Side.BUY else bid
        if value is None:
            raise OrderRejected(f"{info.instrument.symbol}: no live quote; subscribe to ticks first")
        return mp.price_from_int(value, info.instrument.digits)

    async def modify_order(self, order_id: str, changes: OrderChanges) -> BrokerAck:
        if changes.is_empty:
            raise OrderRejected("no changes requested")
        if changes.remove_sl or changes.remove_tp:
            raise OrderRejected("removing SL/TP from a pending order is not supported by the ctrader adapter")
        wire = self._orders.get(int(order_id))
        if wire is None:
            await self._reconcile()
            wire = self._orders.get(int(order_id))
        if wire is None:
            raise OrderNotFound(order_id)
        info = self._by_id[wire.tradeData.symbolId]
        digits = info.instrument.digits
        msg = msgs.ProtoOAAmendOrderReq(ctidTraderAccountId=self._ctid(), orderId=int(order_id))
        if changes.volume is not None:
            msg.volume = mp.lots_to_wire(changes.volume, info.lot_size)
        if changes.limit_price is not None:
            msg.limitPrice = mp.price_to_double(changes.limit_price, digits)
        if changes.stop_price is not None:
            msg.stopPrice = mp.price_to_double(changes.stop_price, digits)
        if changes.sl is not None:
            msg.stopLoss = mp.price_to_double(changes.sl, digits)
        if changes.tp is not None:
            msg.takeProfit = mp.price_to_double(changes.tp, digits)
        if changes.expiry is not None:
            msg.expirationTimestamp = to_epoch_ms(changes.expiry)
        await self._req(msg, msgs.ProtoOAExecutionEvent)
        return BrokerAck(ts=self._now(), client_order_id=wire.clientOrderId or None, broker_order_id=order_id)

    async def cancel_order(self, order_id: str) -> BrokerAck:
        await self._req(
            msgs.ProtoOACancelOrderReq(ctidTraderAccountId=self._ctid(), orderId=int(order_id)),
            msgs.ProtoOAExecutionEvent,
        )
        return BrokerAck(ts=self._now(), broker_order_id=order_id)

    async def modify_position(self, position_id: str, sl: Decimal | None, tp: Decimal | None) -> BrokerAck:
        wire = await self._wire_position(position_id)
        digits = self._by_id[wire.tradeData.symbolId].instrument.digits
        msg = msgs.ProtoOAAmendPositionSLTPReq(ctidTraderAccountId=self._ctid(), positionId=int(position_id))
        if sl is not None:
            msg.stopLoss = mp.price_to_double(sl, digits)
        if tp is not None:
            msg.takeProfit = mp.price_to_double(tp, digits)
        await self._req(msg, msgs.ProtoOAExecutionEvent)
        return BrokerAck(ts=self._now(), position_id=position_id)

    async def close_position(self, position_id: str, volume: Decimal | None = None) -> BrokerAck:
        wire = await self._wire_position(position_id)
        info = self._by_id[wire.tradeData.symbolId]
        wire_volume = wire.tradeData.volume if volume is None else mp.lots_to_wire(volume, info.lot_size)
        if wire_volume > wire.tradeData.volume:
            raise InvalidVolume(f"cannot close {volume} lots of a {wire.tradeData.volume}-cent position")
        ev = await self._req(
            msgs.ProtoOAClosePositionReq(
                ctidTraderAccountId=self._ctid(), positionId=int(position_id), volume=wire_volume
            ),
            msgs.ProtoOAExecutionEvent,
        )
        return BrokerAck(
            ts=self._now(),
            position_id=position_id,
            broker_order_id=str(ev.order.orderId) if ev.HasField("order") else None,
        )

    async def _wire_position(self, position_id: str) -> model.ProtoOAPosition:
        pid = int(position_id)
        if pid not in self._positions:
            await self._reconcile()
        wire = self._positions.get(pid)
        if wire is None or wire.tradeData.symbolId not in self._by_id:
            raise PositionNotFound(position_id)
        return wire

    # ------------------------------------------------------------------ inbound events

    def _emit_trade(self, event: DomainEvent) -> None:
        if self._connected and not self._awaiting_snapshot:
            self._events.put_nowait(event)

    def _on_message(self, message: Message) -> None:
        if isinstance(message, msgs.ProtoOASpotEvent):
            self._on_spot(message)
        elif isinstance(message, msgs.ProtoOAExecutionEvent):
            self._on_execution(message)
        elif isinstance(message, msgs.ProtoOATraderUpdatedEvent):
            self._trader = message.trader
            self._spawn(self._emit_account())
        elif isinstance(message, msgs.ProtoOAMarginChangedEvent):
            pos = self._positions.get(message.positionId)
            if pos is not None:
                pos.usedMargin = message.usedMargin
        elif isinstance(message, msgs.ProtoOAAccountsTokenInvalidatedEvent):
            _log.warning("ctrader_token_invalidated", reason=message.reason)
            self._tokens = replace(self._tokens, expires_at=None)  # force a refresh on reconnect
            self._spawn(self.drop_connection())
        elif isinstance(message, msgs.ProtoOAClientDisconnectEvent | msgs.ProtoOAAccountDisconnectEvent):
            _log.warning("ctrader_server_disconnect", message=type(message).__name__)
            self._spawn(self.drop_connection())
        elif isinstance(message, msgs.ProtoOAOrderErrorEvent):
            _log.warning("ctrader_order_error", code=message.errorCode, order_id=message.orderId)

    def _on_spot(self, ev: msgs.ProtoOASpotEvent) -> None:
        info = self._by_id.get(ev.symbolId)
        if info is None:
            return
        bid, ask = self._quotes.get(ev.symbolId, (None, None))
        bid = ev.bid if ev.HasField("bid") else bid  # bid and ask are optional: keep the last of each
        ask = ev.ask if ev.HasField("ask") else ask
        self._quotes[ev.symbolId] = (bid, ask)
        if bid is None or ask is None or not self._connected:
            return
        now = self._now()
        broker_ts = from_epoch_ms(ev.timestamp) if ev.HasField("timestamp") else None
        digits = info.instrument.digits
        tick = Tick(
            symbol=info.instrument.symbol,
            bid=mp.price_from_int(bid, digits),
            ask=mp.price_from_int(ask, digits),
            ts=broker_ts or now,
            recv_ts=now,
            recv_mono_ns=time.monotonic_ns(),
            broker_ts=broker_ts,
        )
        if info.instrument.symbol in self._subscribed:
            self._events.put_nowait(TickEvent(tick=tick, broker=self.name))

    def _on_execution(self, ev: msgs.ProtoOAExecutionEvent) -> None:
        et = ev.executionType
        now = self._now()
        if ev.HasField("order"):
            self._handle_order(ev, now)
        if ev.HasField("deal") and et in (model.ORDER_FILLED, model.ORDER_PARTIAL_FILL):
            self._handle_deal(ev, now)
        if ev.HasField("position"):
            self._handle_position(ev.position, now)
        if et in (model.ORDER_FILLED, model.ORDER_PARTIAL_FILL, model.SWAP, model.DEPOSIT_WITHDRAW):
            self._spawn(self._emit_account())

    def _handle_order(self, ev: msgs.ProtoOAExecutionEvent, now: datetime) -> None:
        wire = ev.order
        info = self._by_id.get(wire.tradeData.symbolId)
        if info is None or mp.is_internal_order(wire):
            return
        et = ev.executionType
        if et == model.ORDER_CANCEL_REJECTED:
            _log.warning("ctrader_cancel_rejected", order_id=wire.orderId, code=ev.errorCode)
            return
        try:
            order = mp.order_from_wire(wire, info, reason=ev.errorCode or None)
        except ValueError as exc:
            _log.warning("ctrader_order_unmappable", order_id=wire.orderId, error=str(exc))
            return
        if wire.clientOrderId:
            self._cid_to_order.setdefault(wire.clientOrderId, str(wire.orderId))
        if order.status in (OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED) or (
            order.status is OrderStatus.ACCEPTED and wire.orderType in mp.PENDING_WIRE_TYPES
        ):
            self._orders[wire.orderId] = wire
        else:
            self._orders.pop(wire.orderId, None)
        if et == model.ORDER_ACCEPTED and order.status is OrderStatus.WORKING:
            accepted = replace(
                order,
                status=OrderStatus.ACCEPTED,
                history=(StatusChange(status=OrderStatus.ACCEPTED, ts=now),),
            )
            self._emit_trade(
                OrderEvent(
                    account_id=self._s.account_id, order=accepted, previous_status=OrderStatus.NEW, ts=now
                )
            )
            order = replace(
                order, history=(*accepted.history, StatusChange(status=OrderStatus.WORKING, ts=now))
            )
            previous: OrderStatus | None = OrderStatus.ACCEPTED
        elif et == model.ORDER_ACCEPTED:
            previous = OrderStatus.NEW
        else:
            previous = None
        self._emit_trade(
            OrderEvent(account_id=self._s.account_id, order=order, previous_status=previous, ts=now)
        )

    def _handle_deal(self, ev: msgs.ProtoOAExecutionEvent, now: datetime) -> None:
        deal = ev.deal
        if deal.dealStatus not in (model.FILLED, model.PARTIALLY_FILLED):
            return
        info = self._by_id.get(deal.symbolId)
        if info is None:
            return
        digits_money = deal.moneyDigits or (self._trader.moneyDigits if self._trader else 2)
        close = deal.closePositionDetail if deal.HasField("closePositionDetail") else None
        internal = ev.HasField("order") and mp.is_internal_order(ev.order)
        cid = ev.order.clientOrderId if ev.HasField("order") and not internal else ""
        fill = Fill(
            fill_id=str(deal.dealId),
            client_order_id=cid or None,
            broker_order_id=str(deal.orderId),
            position_id=str(deal.positionId),
            symbol=info.instrument.symbol,
            side=mp.side_from_wire(deal.tradeSide),
            volume=mp.lots_from_wire(deal.filledVolume, info.lot_size),
            price=mp.price_from_double(deal.executionPrice, info.instrument.digits),
            ts=from_epoch_ms(deal.executionTimestamp),
            commission=mp.money(deal.commission, digits_money),
            swap=mp.money(close.swap, close.moneyDigits or digits_money) if close else ZERO,
            realized_pnl=mp.money(close.grossProfit, close.moneyDigits or digits_money) if close else None,
        )
        self._emit_trade(FillEvent(account_id=self._s.account_id, fill=fill, ts=now))

    def _handle_position(self, wire: model.ProtoOAPosition, now: datetime) -> None:
        info = self._by_id.get(wire.tradeData.symbolId)
        if info is None or wire.positionStatus == model.POSITION_STATUS_CREATED:
            return
        previous = self._positions.get(wire.positionId)
        if wire.positionStatus == model.POSITION_STATUS_CLOSED or wire.tradeData.volume == 0:
            self._positions.pop(wire.positionId, None)
            if previous is None:
                return
            # The closed position reports volume 0; publish the last open view with the CLOSED change.
            pos = mp.position_from_wire(previous, info)
            self._emit_trade(
                PositionEvent(
                    account_id=self._s.account_id, position=pos, change=PositionChange.CLOSED, ts=now
                )
            )
            return
        if wire.positionStatus != model.POSITION_STATUS_OPEN:
            return
        self._positions[wire.positionId] = wire
        if previous is None:
            change = PositionChange.OPENED
        elif wire.tradeData.volume < previous.tradeData.volume:
            change = PositionChange.PARTIALLY_CLOSED
        else:
            change = PositionChange.MODIFIED
        pos = mp.position_from_wire(wire, info)
        self._emit_trade(PositionEvent(account_id=self._s.account_id, position=pos, change=change, ts=now))

    async def _emit_account(self) -> None:
        if not self._connected or self._awaiting_snapshot:
            return
        account = await self.account()
        self._emit_trade(AccountEvent(account_id=self._s.account_id, state=account, ts=account.ts))

    async def _poll_account(self) -> None:
        """Background upkeep: refresh the access token before it expires (long-running connections never
        reconnect on their own) and publish account state while positions are open."""
        while True:
            await asyncio.sleep(self._s.account_poll_s)
            transport = self._transport
            if (
                self._connected
                and transport is not None
                and self._tokens.expires_within(self._now(), self._s.token_refresh_margin)
            ):
                try:
                    await self._refresh_tokens(transport)
                except BrokerError as exc:
                    _log.warning("ctrader_token_refresh_failed", error=str(exc))
            if self._connected and not self._awaiting_snapshot and self._positions:
                with contextlib.suppress(BrokerError):
                    await self._emit_account()

    def _spawn(self, coro: Coroutine[None, None, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if (
            not task.cancelled()
            and (exc := task.exception()) is not None
            and not isinstance(exc, BrokerError)
        ):
            _log.error("ctrader_task_error", error=repr(exc))
