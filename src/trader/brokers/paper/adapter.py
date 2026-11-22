"""PaperBroker: simulated execution against ticks from a ``MarketDataSource``.

Behaves like an asynchronous broker (``sync_order_result=False``): requests are validated synchronously
(raising ``domain.errors``) and ACKed; outcomes arrive later through ``events()``. Like the real brokers
it has no event replay: while disconnected, and after reconnect until ``snapshot()`` is called, it emits
no trade-related events (state keeps changing broker-side).
See docs/adr/0003-broker-interface-and-paper-broker.md.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import AsyncIterator, Coroutine, Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final

from trader.brokers.base import BrokerAck, Capabilities, MarketDataSource, OrderChanges, RateLimits
from trader.brokers.ratelimit import TokenBucket
from trader.domain.accounting import margin_required, pnl_quote, weighted_entry
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
    BrokerUnavailable,
    ConversionUnavailable,
    InvalidPrice,
    InvalidVolume,
    MarketClosed,
    OrderNotFound,
    OrderRejected,
    PositionNotFound,
    RateLimited,
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
from trader.domain.fx import conversion_rate
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
    Tick,
)
from trader.domain.money import ZERO
from trader.domain.time import Clock, ensure_utc
from trader.observability.logging import get_logger

from .settings import PaperSettings

_log = get_logger("trader.brokers.paper")

_PENDING_TYPES: Final = frozenset({OrderType.LIMIT, OrderType.STOP, OrderType.STOP_LIMIT})
_OPEN_STATUSES: Final = frozenset({OrderStatus.ACCEPTED, OrderStatus.WORKING})


def _advance(order: Order, status: OrderStatus, ts: datetime, reason: str | None = None) -> Order:
    return replace(
        order,
        status=status,
        history=(*order.history, StatusChange(status=status, ts=ts, reason=reason)),
        rejection_reason=reason if status is OrderStatus.REJECTED else order.rejection_reason,
    )


class PaperBroker:
    def __init__(
        self,
        source: MarketDataSource,
        settings: PaperSettings,
        clock: Clock,
        *,
        name: str = "paper",
    ) -> None:
        self._source = source
        self._settings = settings
        self._clock = clock
        self._name = name
        self._capabilities = Capabilities(
            order_types=frozenset(OrderType),
            native_sl_tp=True,
            native_trailing_stop=False,  # emulated by the OMS (phase 5)
            partial_close=True,
            close_by=False,
            hedging=settings.hedging,
            netting=not settings.hedging,
            modify_pending=True,
            server_side_expiry=True,
            tick_stream=True,
            candle_history={tf: timedelta(days=30) for tf in Timeframe if tf.duration is not None},
            max_candles_per_request=1000,
            sync_order_result=False,
            idempotent_submit=True,
        )
        self._bucket = TokenBucket(settings.rate_limit) if settings.rate_limit else None

        self._instruments: dict[str, Instrument] = {}
        self._last: dict[str, Tick] = {}
        self._subscribed: set[str] = set()
        self._orders: dict[str, Order] = {}  # broker_order_id -> latest version
        self._by_client_id: dict[str, str] = {}
        self._stop_triggered: set[str] = set()
        self._positions: dict[str, Position] = {}
        self._rates: dict[tuple[str, str], Decimal] = {}
        self._balance = settings.starting_balance
        self._fills: list[Fill] = []
        self._ids = itertools.count(1)
        # Ids must stay unique across process restarts (the fill ledger and journal dedupe by id).
        self._run = new_client_order_id()[-6:].lower()

        self._events: asyncio.Queue[DomainEvent | None] = asyncio.Queue()
        self._events_consumed = False
        self._connected = False
        self._awaiting_snapshot = True
        self._tick_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._now_ts: datetime | None = None
        self._prev_tick_ts: datetime | None = None
        self._last_account_event: datetime | None = None

    # ------------------------------------------------------------------ properties

    @property
    def name(self) -> str:
        return self._name

    @property
    def capabilities(self) -> Capabilities:
        return self._capabilities

    @property
    def connected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------------ lifecycle

    async def connect(self) -> None:
        if self._connected:
            return
        if not self._instruments:
            self._instruments = {i.symbol: i for i in await self._source.instruments()}
        if self._tick_task is None:
            self._tick_task = asyncio.create_task(self._run_ticks(), name=f"{self._name}-ticks")
        self._set_connection(ConnectionState.UP, "connected")

    async def close(self) -> None:
        tasks = [t for t in (self._tick_task, *self._tasks) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tick_task = None
        if self._connected:
            self._set_connection(ConnectionState.DOWN, "closed")
        self._events.put_nowait(None)

    async def simulate_disconnect(self, reason: str = "simulated disconnect") -> None:
        """Test hook: drop the 'connection'. The simulated broker keeps executing on ticks meanwhile."""
        if self._connected:
            self._set_connection(ConnectionState.DOWN, reason)

    async def simulate_reconnect(self) -> None:
        if not self._connected:
            self._set_connection(ConnectionState.UP, "reconnected")

    def _set_connection(self, state: ConnectionState, reason: str) -> None:
        self._connected = state is ConnectionState.UP
        if self._connected:
            self._awaiting_snapshot = True  # no trade events until the OMS reconciles
        self._events.put_nowait(
            ConnectionEvent(
                account_id=self._settings.account_id,
                broker=self._name,
                state=state,
                ts=self._now(),
                reason=reason,
            )
        )

    async def events(self) -> AsyncIterator[DomainEvent]:
        if self._events_consumed:
            raise RuntimeError("events() supports a single consumer (the account's OMS)")
        self._events_consumed = True
        try:
            while (event := await self._events.get()) is not None:
                yield event
        finally:
            self._events_consumed = False  # a restarted process may attach again

    def discard_events(self) -> int:
        """Test hook: drop events nobody consumed (they died with the connection). Returns how many."""
        n = 0
        while not self._events.empty():
            self._events.get_nowait()
            n += 1
        return n

    # ------------------------------------------------------------------ queries

    async def instruments(self) -> list[Instrument]:
        self._request()
        return list(self._instruments.values())

    async def account(self) -> AccountState:
        self._request()
        return self._account_state()

    async def positions(self) -> list[Position]:
        self._request()
        return self._position_views()

    async def open_orders(self) -> list[Order]:
        self._request()
        return self._open_orders()

    async def snapshot(self) -> BrokerSnapshot:
        self._request()
        snap = BrokerSnapshot(
            account=self._account_state(),
            positions=tuple(self._position_views()),
            orders=tuple(self._open_orders()),
            ts=self._now(),
        )
        self._awaiting_snapshot = False
        return snap

    async def find_order(self, client_order_id: str, since: datetime) -> Order | None:
        self._request()
        broker_id = self._by_client_id.get(client_order_id)
        return self._orders.get(broker_id) if broker_id is not None else None

    async def limits(self) -> RateLimits:
        limit = self._settings.rate_limit
        return RateLimits(buckets=(limit,) if limit else ())

    async def candles(self, symbol: str, tf: Timeframe, start: datetime, end: datetime) -> list[Bar]:
        self._request()
        self._instrument(symbol)
        if ensure_utc(start) >= ensure_utc(end):
            raise ValueError("candles: start must be before end")
        return await self._source.candles(symbol, tf, start, end)

    async def subscribe_ticks(self, symbols: Sequence[str]) -> None:
        self._request()
        for symbol in symbols:
            self._instrument(symbol)
        await self._source.subscribe(symbols)
        self._subscribed.update(symbols)

    # ------------------------------------------------------------------ trading requests

    async def place_order(self, req: OrderRequest) -> BrokerAck:
        self._request()
        if (existing_id := self._by_client_id.get(req.client_order_id)) is not None:
            return BrokerAck(
                ts=self._now(),
                client_order_id=req.client_order_id,
                broker_order_id=existing_id,
                duplicate=True,
            )
        inst = self._instrument(req.symbol)
        if not inst.is_valid_volume(req.volume):
            raise InvalidVolume(f"{req.symbol}: invalid volume {req.volume}")
        if req.order_type is not OrderType.MARKET and req.time_in_force in (TimeInForce.IOC, TimeInForce.FOK):
            raise OrderRejected(f"{req.time_in_force} is only supported for MARKET orders")
        tick = self._tradable_tick(inst)
        for value in (req.limit_price, req.stop_price, req.sl, req.tp):
            if value is not None:
                inst.check_price(value)
        bid, ask = self._quote(tick)
        self._check_pending_price(req, bid, ask)
        entry = req.entry_price if req.entry_price is not None else (ask if req.side is Side.BUY else bid)
        self._check_sl_tp(req.side, entry, req.sl, req.tp)
        self._ensure_conversions(inst)

        now = self._now()
        broker_id = f"PO{self._run}-{next(self._ids)}"
        order = _advance(replace(Order.new(req, now), broker_order_id=broker_id), OrderStatus.ACCEPTED, now)
        self._orders[broker_id] = order
        self._by_client_id[req.client_order_id] = broker_id
        self._emit_order(order, OrderStatus.NEW)
        self._spawn(self._process_new(broker_id))
        return BrokerAck(ts=now, client_order_id=req.client_order_id, broker_order_id=broker_id)

    async def modify_order(self, order_id: str, changes: OrderChanges) -> BrokerAck:
        self._request()
        order = self._open_order(order_id)
        if changes.is_empty:
            raise OrderRejected("no changes requested")
        req = order.request
        inst = self._instrument(req.symbol)
        if changes.volume is not None and not inst.is_valid_volume(changes.volume):
            raise InvalidVolume(f"{req.symbol}: invalid volume {changes.volume}")
        try:
            new_req = replace(
                req,
                volume=changes.volume if changes.volume is not None else req.volume,
                limit_price=changes.limit_price if changes.limit_price is not None else req.limit_price,
                stop_price=changes.stop_price if changes.stop_price is not None else req.stop_price,
                sl=None if changes.remove_sl else (changes.sl if changes.sl is not None else req.sl),
                tp=None if changes.remove_tp else (changes.tp if changes.tp is not None else req.tp),
                expiry=changes.expiry if changes.expiry is not None else req.expiry,
            )
        except ValueError as exc:
            raise InvalidPrice(str(exc)) from exc
        for value in (new_req.limit_price, new_req.stop_price, new_req.sl, new_req.tp):
            if value is not None:
                inst.check_price(value)
        bid, ask = self._quote(self._tradable_tick(inst))
        if order.broker_order_id not in self._stop_triggered:
            self._check_pending_price(new_req, bid, ask)
        assert new_req.entry_price is not None
        self._check_sl_tp(new_req.side, new_req.entry_price, new_req.sl, new_req.tp)

        updated = replace(order, request=new_req)
        self._orders[order_id] = updated
        self._emit_order(updated, order.status)
        return BrokerAck(ts=self._now(), client_order_id=req.client_order_id, broker_order_id=order_id)

    async def cancel_order(self, order_id: str) -> BrokerAck:
        self._request()
        order = self._open_order(order_id)
        self._finish(order, OrderStatus.CANCELLED, "cancelled by request")
        return BrokerAck(ts=self._now(), client_order_id=order.client_order_id, broker_order_id=order_id)

    async def modify_position(self, position_id: str, sl: Decimal | None, tp: Decimal | None) -> BrokerAck:
        self._request()
        pos = self._position(position_id)
        inst = self._instrument(pos.symbol)
        for value in (sl, tp):
            if value is not None:
                inst.check_price(value)
        bid, ask = self._quote(self._tradable_tick(inst))
        self._check_sl_tp(pos.side, bid if pos.side is Side.BUY else ask, sl, tp)
        updated = replace(pos, sl=sl, tp=tp)
        self._positions[position_id] = updated
        self._emit_trade(self._position_event(updated, PositionChange.MODIFIED))
        return BrokerAck(ts=self._now(), position_id=position_id)

    async def close_position(self, position_id: str, volume: Decimal | None = None) -> BrokerAck:
        self._request()
        pos = self._position(position_id)
        inst = self._instrument(pos.symbol)
        vol = pos.volume if volume is None else volume
        if vol > pos.volume or not inst.is_valid_volume(vol):
            raise InvalidVolume(f"cannot close {vol} of {pos.volume} lots")
        if vol < pos.volume and pos.volume - vol < inst.min_volume:
            raise InvalidVolume(f"partial close would leave less than min volume {inst.min_volume}")
        self._tradable_tick(inst)
        self._spawn(self._process_close(position_id, vol))
        return BrokerAck(ts=self._now(), position_id=position_id)

    # ------------------------------------------------------------------ execution

    async def _process_new(self, broker_id: str) -> None:
        await self._latency()
        order = self._orders[broker_id]
        if order.status not in _OPEN_STATUSES:
            return
        if order.request.order_type is OrderType.MARKET:
            tick = self._last[order.request.symbol]
            self._execute(order, tick, slippage=True)
        else:
            working = _advance(order, OrderStatus.WORKING, self._now())
            self._orders[broker_id] = working
            self._emit_order(working, order.status)

    async def _process_close(self, position_id: str, volume: Decimal) -> None:
        await self._latency()
        pos = self._positions.get(position_id)
        if pos is None:
            return  # closed meanwhile (SL/TP)
        volume = min(volume, pos.volume)
        tick = self._last[pos.symbol]
        price = self._fill_price(pos.side.opposite, tick, slippage=True)
        self._close(pos, volume, price, self._now(), reason="close request")

    def _execute(self, order: Order, tick: Tick, *, slippage: bool) -> None:
        req = order.request
        inst = self._instruments[req.symbol]
        slip = inst.tick_size * self._settings.slippage_points if slippage else ZERO
        if req.max_slippage is not None and slip > req.max_slippage:
            self._finish(
                order, OrderStatus.REJECTED, f"slippage {slip} exceeds max_slippage {req.max_slippage}"
            )
            return
        price = self._fill_price(req.side, tick, slippage=slippage)
        if not self._has_margin(inst, req.side, req.volume, price):
            self._finish(order, OrderStatus.REJECTED, "insufficient margin")
            return
        now = self._now()
        filled = replace(
            _advance(order, OrderStatus.FILLED, now),
            filled_volume=req.volume,
            avg_fill_price=price,
        )
        self._orders[order.broker_order_id or ""] = filled
        self._stop_triggered.discard(order.broker_order_id or "")
        self._open_or_net(filled, price, now)
        self._emit_order(filled, order.status)
        self._emit_account()

    def _open_or_net(self, order: Order, price: Decimal, ts: datetime) -> None:
        req = order.request
        existing = None if self._settings.hedging else self._netting_position(req.symbol)

        if existing is not None and existing.side is not req.side:
            close_vol = min(existing.volume, req.volume)
            self._close(existing, close_vol, price, ts, reason="netted", order=order)
            remainder = req.volume - close_vol
            if remainder > 0:
                self._open(order, remainder, price, ts)
            return
        commission = self._settings.commission_per_lot * req.volume
        if existing is not None:
            self._balance -= commission
            merged = replace(
                existing,
                volume=existing.volume + req.volume,
                entry_price=weighted_entry(existing.volume, existing.entry_price, req.volume, price),
                commission=existing.commission - commission,
            )
            self._positions[existing.position_id] = merged
            self._emit_fill(order, merged.position_id, req.volume, price, ts, commission=-commission)
            self._emit_trade(self._position_event(merged, PositionChange.MODIFIED))
            return
        self._open(order, req.volume, price, ts)

    def _open(self, order: Order, volume: Decimal, price: Decimal, ts: datetime) -> None:
        req = order.request
        commission = self._settings.commission_per_lot * volume
        self._balance -= commission
        pos = Position(
            position_id=f"PP{self._run}-{next(self._ids)}",
            symbol=req.symbol,
            side=req.side,
            volume=volume,
            entry_price=price,
            opened_ts=ts,
            strategy_id=req.strategy_id,
            sl=req.sl,
            tp=req.tp,
            commission=-commission,
        )
        self._positions[pos.position_id] = pos
        self._emit_fill(order, pos.position_id, volume, price, ts, commission=-commission)
        self._emit_trade(self._position_event(pos, PositionChange.OPENED))

    def _close(
        self,
        pos: Position,
        volume: Decimal,
        price: Decimal,
        ts: datetime,
        *,
        reason: str,
        order: Order | None = None,
    ) -> None:
        inst = self._instruments[pos.symbol]
        rate = self._rate(inst.quote_currency)
        gross = pnl_quote(pos.side, volume, pos.entry_price, price, inst.contract_size) * rate
        swap = pos.swap * volume / pos.volume
        commission = self._settings.commission_per_lot * volume
        self._balance += gross + swap - commission

        fill = Fill(
            fill_id=f"PD{self._run}-{next(self._ids)}",
            client_order_id=order.client_order_id if order else None,
            broker_order_id=(order.broker_order_id or "") if order else f"PO{self._run}-{next(self._ids)}",
            position_id=pos.position_id,
            symbol=pos.symbol,
            side=pos.side.opposite,
            volume=volume,
            price=price,
            ts=ts,
            commission=-commission,
            swap=swap,
            realized_pnl=gross,
        )
        self._record_fill(fill, ts)
        if volume == pos.volume:
            del self._positions[pos.position_id]
            self._emit_trade(self._position_event(replace(pos, unrealized_pnl=ZERO), PositionChange.CLOSED))
        else:
            remaining = replace(pos, volume=pos.volume - volume, swap=pos.swap - swap)
            self._positions[pos.position_id] = remaining
            self._emit_trade(self._position_event(remaining, PositionChange.PARTIALLY_CLOSED))
        _log.debug(
            "paper_close", position_id=pos.position_id, volume=str(volume), price=str(price), reason=reason
        )
        if order is None:
            self._emit_account()

    def _finish(self, order: Order, status: OrderStatus, reason: str) -> None:
        done = _advance(order, status, self._now(), reason)
        self._orders[order.broker_order_id or ""] = done
        self._stop_triggered.discard(order.broker_order_id or "")
        self._emit_order(done, order.status)

    # ------------------------------------------------------------------ tick loop

    async def _run_ticks(self) -> None:
        async for tick in self._source.ticks():
            try:
                self._on_tick(tick)
            except Exception:  # keep the simulated market alive; the error is logged
                _log.exception("paper_tick_error", symbol=tick.symbol)

    def _on_tick(self, tick: Tick) -> None:
        self._last[tick.symbol] = tick
        if self._now_ts is None or tick.ts > self._now_ts:
            self._now_ts = tick.ts
        if self._connected and tick.symbol in self._subscribed:
            self._events.put_nowait(TickEvent(tick=tick, broker=self._name))
        self._apply_rollovers(tick.ts)
        self._expire_orders(tick.ts)
        self._trigger_pending(tick)
        self._trigger_sl_tp(tick)
        if self._positions and (
            self._last_account_event is None
            or tick.ts - self._last_account_event >= self._settings.account_event_interval
        ):
            self._emit_account()

    def _trigger_pending(self, tick: Tick) -> None:
        bid, ask = self._quote(tick)
        for order in [o for o in self._orders.values() if o.status is OrderStatus.WORKING]:
            req = order.request
            if req.symbol != tick.symbol:
                continue
            buy = req.side is Side.BUY
            oid = order.broker_order_id or ""
            if req.order_type is OrderType.STOP_LIMIT and oid not in self._stop_triggered:
                assert req.stop_price is not None
                if (ask >= req.stop_price) if buy else (bid <= req.stop_price):
                    self._stop_triggered.add(oid)
            if req.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
                if req.order_type is OrderType.STOP_LIMIT and oid not in self._stop_triggered:
                    continue
                assert req.limit_price is not None
                if (ask <= req.limit_price) if buy else (bid >= req.limit_price):
                    self._execute(order, tick, slippage=False)
            elif req.order_type is OrderType.STOP:
                assert req.stop_price is not None
                if (ask >= req.stop_price) if buy else (bid <= req.stop_price):
                    self._execute(order, tick, slippage=True)

    def _trigger_sl_tp(self, tick: Tick) -> None:
        bid, ask = self._quote(tick)
        for pos in [p for p in self._positions.values() if p.symbol == tick.symbol]:
            price = bid if pos.side is Side.BUY else ask  # price the position closes at
            long = pos.side is Side.BUY
            sl_hit = pos.sl is not None and (price <= pos.sl if long else price >= pos.sl)
            tp_hit = pos.tp is not None and (price >= pos.tp if long else price <= pos.tp)
            if sl_hit:  # checked first: on a gap through both, assume the worse outcome
                fill = self._fill_price(pos.side.opposite, tick, slippage=True)
                self._close(pos, pos.volume, fill, tick.ts, reason="stop loss")
            elif tp_hit:
                self._close(pos, pos.volume, price, tick.ts, reason="take profit")

    def _expire_orders(self, ts: datetime) -> None:
        for order in [o for o in self._orders.values() if o.status in _OPEN_STATUSES]:
            req = order.request
            accepted = order.history[0].ts
            day_end = accepted.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            if (req.time_in_force is TimeInForce.GTD and req.expiry is not None and ts >= req.expiry) or (
                req.time_in_force is TimeInForce.DAY and ts >= day_end
            ):
                self._finish(order, OrderStatus.EXPIRED, "expired")

    def _apply_rollovers(self, ts: datetime) -> None:
        prev, self._prev_tick_ts = self._prev_tick_ts, ts
        if prev is None or ts <= prev or not self._positions:
            return
        hour = self._settings.rollover_hour_utc
        boundary = prev.replace(hour=hour, minute=0, second=0, microsecond=0)
        if boundary <= prev:
            boundary += timedelta(days=1)
        while boundary <= ts:
            weekday = boundary.weekday()
            if weekday < 5:  # no rollover on Saturday/Sunday; Wednesday carries the weekend
                nights = 3 if weekday == self._settings.triple_swap_weekday else 1
                for pid, pos in list(self._positions.items()):
                    rate = (
                        self._settings.swap_long_per_lot
                        if pos.side is Side.BUY
                        else self._settings.swap_short_per_lot
                    )
                    self._positions[pid] = replace(pos, swap=pos.swap + rate * pos.volume * nights)
            boundary += timedelta(days=1)

    # ------------------------------------------------------------------ helpers

    def _request(self) -> None:
        if not self._connected:
            raise BrokerUnavailable(f"{self._name} is not connected")
        if self._bucket is not None and (wait_ms := self._bucket.try_acquire()) > 0:
            raise RateLimited("paper rate limit exceeded", retry_after_ms=wait_ms)

    def _now(self) -> datetime:
        now = self._clock.now()
        return self._now_ts if self._now_ts is not None and self._now_ts > now else now

    def _spawn(self, coro: Coroutine[None, None, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and (exc := task.exception()) is not None:
            _log.error("paper_task_error", error=repr(exc))

    async def _latency(self) -> None:
        await asyncio.sleep(self._settings.latency_ms / 1000)

    def _instrument(self, symbol: str) -> Instrument:
        inst = self._instruments.get(symbol)
        if inst is None:
            raise UnknownSymbol(f"{symbol} is not available on {self._name}")
        return inst

    def _tradable_tick(self, inst: Instrument) -> Tick:
        tick = self._last.get(inst.symbol)
        if tick is None:
            raise MarketClosed(f"{inst.symbol}: no price yet")
        if not inst.is_open(self._now()):
            raise MarketClosed(f"{inst.symbol}: outside trading session")
        return tick

    def _open_order(self, order_id: str) -> Order:
        order = self._orders.get(order_id)
        if order is None or order.status not in _OPEN_STATUSES:
            raise OrderNotFound(order_id)
        return order

    def _position(self, position_id: str) -> Position:
        pos = self._positions.get(position_id)
        if pos is None:
            raise PositionNotFound(position_id)
        return pos

    def _netting_position(self, symbol: str) -> Position | None:
        return next((p for p in self._positions.values() if p.symbol == symbol), None)

    def _quote(self, tick: Tick) -> tuple[Decimal, Decimal]:
        """Bid/ask after the configured spread widening."""
        widen = self._settings.extra_spread_points
        if widen == 0:
            return tick.bid, tick.ask
        half = self._instruments[tick.symbol].tick_size * widen / 2
        return tick.bid - half, tick.ask + half

    def _fill_price(self, side: Side, tick: Tick, *, slippage: bool) -> Decimal:
        bid, ask = self._quote(tick)
        price = ask if side is Side.BUY else bid
        if slippage and self._settings.slippage_points:
            price += side.sign * self._instruments[tick.symbol].tick_size * self._settings.slippage_points
        return price

    @staticmethod
    def _check_pending_price(req: OrderRequest, bid: Decimal, ask: Decimal) -> None:
        """Pending orders must rest on the correct side of the market (as forex brokers enforce)."""
        if req.order_type not in _PENDING_TYPES:
            return
        buy = req.side is Side.BUY
        if req.order_type is OrderType.LIMIT:
            assert req.limit_price is not None
            ok = req.limit_price < ask if buy else req.limit_price > bid
        else:
            assert req.stop_price is not None
            ok = req.stop_price > ask if buy else req.stop_price < bid
        if not ok:
            raise InvalidPrice(f"{req.side} {req.order_type} price is on the wrong side of the market")

    @staticmethod
    def _check_sl_tp(side: Side, ref: Decimal, sl: Decimal | None, tp: Decimal | None) -> None:
        buy = side is Side.BUY
        if sl is not None and not (sl < ref if buy else sl > ref):
            raise InvalidPrice(f"SL {sl} is on the wrong side of {ref} for {side}")
        if tp is not None and not (tp > ref if buy else tp < ref):
            raise InvalidPrice(f"TP {tp} is on the wrong side of {ref} for {side}")

    def _rate(self, ccy: str) -> Decimal:
        key = (ccy, self._settings.currency)
        try:
            rate = conversion_rate(ccy, self._settings.currency, self._last.get)
        except ConversionUnavailable:
            if key in self._rates:
                return self._rates[key]
            raise
        self._rates[key] = rate
        return rate

    def _ensure_conversions(self, inst: Instrument) -> None:
        self._rate(inst.quote_currency)
        self._rate(inst.margin_currency)

    def _margin_used(self) -> Decimal:
        total = ZERO
        for pos in self._positions.values():
            inst = self._instruments[pos.symbol]
            tick = self._last.get(pos.symbol)
            price = tick.mid if tick is not None else pos.entry_price
            amount, ccy = margin_required(inst, pos.volume, price, self._settings.leverage)
            total += amount * self._rate(ccy)
        return total

    def _unrealized(self, pos: Position) -> Decimal:
        inst = self._instruments[pos.symbol]
        tick = self._last.get(pos.symbol)
        if tick is None:
            return ZERO
        bid, ask = self._quote(tick)
        exit_price = bid if pos.side is Side.BUY else ask
        return pnl_quote(pos.side, pos.volume, pos.entry_price, exit_price, inst.contract_size) * self._rate(
            inst.quote_currency
        )

    def _has_margin(self, inst: Instrument, side: Side, volume: Decimal, price: Decimal) -> bool:
        if not self._settings.hedging:
            existing = self._netting_position(inst.symbol)
            if existing is not None and existing.side is not side and existing.volume >= volume:
                return True  # reduces exposure
        amount, ccy = margin_required(inst, volume, price, self._settings.leverage)
        state = self._account_state()
        return state.free_margin >= amount * self._rate(ccy)

    def _account_state(self) -> AccountState:
        floating = sum((self._unrealized(p) + p.swap for p in self._positions.values()), ZERO)
        equity = self._balance + floating
        margin = self._margin_used()
        return AccountState(
            account_id=self._settings.account_id,
            currency=self._settings.currency,
            balance=self._balance,
            equity=equity,
            margin=margin,
            free_margin=equity - margin,
            leverage=self._settings.leverage,
            ts=self._now(),
            margin_level=equity / margin * 100 if margin > 0 else None,
        )

    def _position_views(self) -> list[Position]:
        return [replace(p, unrealized_pnl=self._unrealized(p)) for p in self._positions.values()]

    def _open_orders(self) -> list[Order]:
        return [o for o in self._orders.values() if o.status in _OPEN_STATUSES]

    # ------------------------------------------------------------------ event emission

    def _emit_trade(self, event: DomainEvent) -> None:
        if self._connected and not self._awaiting_snapshot:
            self._events.put_nowait(event)

    def _emit_order(self, order: Order, previous: OrderStatus | None) -> None:
        self._emit_trade(
            OrderEvent(
                account_id=self._settings.account_id, order=order, previous_status=previous, ts=self._now()
            )
        )

    def _emit_fill(
        self,
        order: Order,
        position_id: str,
        volume: Decimal,
        price: Decimal,
        ts: datetime,
        *,
        commission: Decimal,
    ) -> None:
        fill = Fill(
            fill_id=f"PD{self._run}-{next(self._ids)}",
            client_order_id=order.client_order_id,
            broker_order_id=order.broker_order_id or "",
            position_id=position_id,
            symbol=order.request.symbol,
            side=order.request.side,
            volume=volume,
            price=price,
            ts=ts,
            commission=commission,
        )
        self._record_fill(fill, ts)

    def _record_fill(self, fill: Fill, ts: datetime) -> None:
        """Every deal is kept (``deal_history``) with the balance it left, like a broker statement."""
        fill = replace(fill, balance_after=self._balance)
        self._fills.append(fill)
        self._emit_trade(FillEvent(account_id=self._settings.account_id, fill=fill, ts=ts))

    async def deal_history(self, start: datetime, end: datetime) -> list[Fill]:
        self._request()
        return [f for f in self._fills if start <= f.ts < end]

    def _position_event(self, pos: Position, change: PositionChange) -> PositionEvent:
        return PositionEvent(
            account_id=self._settings.account_id, position=pos, change=change, ts=self._now()
        )

    def _emit_account(self) -> None:
        state = self._account_state()
        self._last_account_event = state.ts
        self._emit_trade(AccountEvent(account_id=self._settings.account_id, state=state, ts=state.ts))
