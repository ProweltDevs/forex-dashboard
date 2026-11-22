"""A minimal in-process cTrader Open API server (plain TCP, same framing and messages) for tests.

It models what the adapter relies on: auth order, token refresh, symbols/assets, spots, execution events
(ACK first, then fill), reconcile, unrealized P&L, trendbars with a range limit, and the 50 req/s limit.
It is not a full simulator: one account, one symbol (EURUSD), hedging, fills at the touch.
"""

from __future__ import annotations

import asyncio
import contextlib
import struct
import time
from dataclasses import dataclass, field

from google.protobuf.message import Message

from trader.brokers.ctrader.proto import OpenApiMessages_pb2 as msgs
from trader.brokers.ctrader.proto import OpenApiModelMessages_pb2 as model
from trader.brokers.ctrader.transport import decode, encode

EURUSD_ID = 1
LOT_SIZE = 10_000_000  # cents of units per lot (100 000 units)
STEP = 100_000  # 0.01 lot in cents
MAX_BARS_PER_REQUEST = 2000


@dataclass
class _Conn:
    writer: asyncio.StreamWriter
    app_authed: bool = False
    account_authed: bool = False
    spots: set[int] = field(default_factory=set)
    window: list[float] = field(default_factory=list)


class FakeCTraderServer:
    def __init__(
        self,
        *,
        ctid: int = 1001,
        client_id: str = "client-id",
        client_secret: str = "client-secret",
        access_token: str = "access-1",
        refresh_token: str = "refresh-1",
        rate_limit_per_s: int = 50,
    ) -> None:
        self.ctid = ctid
        self.client_id, self.client_secret = client_id, client_secret
        self.access_token, self.refresh_token = access_token, refresh_token
        self.rate_limit_per_s = rate_limit_per_s
        self.balance_cents = 1_000_000  # 10 000.00 USD, moneyDigits = 2
        self.bid, self.ask = 110_000, 110_010  # 1/100000 units
        self.positions: dict[int, model.ProtoOAPosition] = {}
        self.orders: dict[int, model.ProtoOAOrder] = {}
        self.client_ids: dict[str, int] = {}
        self.history: list[model.ProtoOAOrder] = []  # finished orders, for ProtoOAOrderListReq
        self.history_chunk = 1000  # records per ProtoOAOrderListRes / ProtoOADealListRes before hasMore
        self.deals: list[model.ProtoOADeal] = []  # for ProtoOADealListReq
        self.history_requests = 0
        self.received: list[str] = []
        # Chaos knobs (tests/chaos): delay execution of new orders, lose the next N order requests on the way
        # in (received, never executed), and every clientOrderId ever sent to us, in order.
        self.ack_delay_s = 0.0
        self.swallow_orders = 0
        self.new_order_requests: list[str] = []
        self.refreshes = 0
        self.rate_limited = 0
        self._ids = iter(range(10_000, 10_000_000))
        self._conns: list[_Conn] = []
        self._server: asyncio.Server | None = None
        self._ticker: asyncio.Task[None] | None = None
        self.port = 0

    # ------------------------------------------------------------------ lifecycle

    async def start(self, port: int = 0) -> int:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", port)
        self.port = self._server.sockets[0].getsockname()[1]
        if self._ticker is None or self._ticker.done():
            self._ticker = asyncio.create_task(self._tick_loop())
        return self.port

    async def go_down(self) -> None:
        """The broker becomes unreachable: connections dropped, new ones refused. State is kept."""
        server, self._server = self._server, None
        if server:
            server.close()  # stop accepting first, then drop: wait_closed() waits for open connections
        await self.drop_all()
        if server:
            await server.wait_closed()

    async def come_back(self) -> None:
        """Reachable again on the same port, with the same accounts, orders and positions."""
        await self.start(self.port)

    async def stop(self) -> None:
        if self._ticker:
            self._ticker.cancel()
        await self.drop_all()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def drop_all(self) -> None:
        for conn in list(self._conns):
            conn.writer.close()
            with contextlib.suppress(Exception):
                await conn.writer.wait_closed()
        self._conns.clear()

    async def _tick_loop(self) -> None:
        i = 0
        while True:
            await asyncio.sleep(0.02)
            i += 1
            self.bid = 110_000 + 2 * (i % 2)
            self.ask = self.bid + 10
            for conn in list(self._conns):
                if EURUSD_ID in conn.spots:
                    self._send(conn, self._spot())

    # ------------------------------------------------------------------ io

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _Conn(writer)
        self._conns.append(conn)
        try:
            while True:
                (length,) = struct.unpack(">I", await reader.readexactly(4))
                inbound = decode(await reader.readexactly(length))
                if inbound is None:
                    continue
                name = type(inbound.message).__name__
                if name == "ProtoHeartbeatEvent":
                    continue
                self.received.append(name)
                self._handle(conn, inbound.message, inbound.client_msg_id)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            if conn in self._conns:
                self._conns.remove(conn)
            writer.close()

    def _send(self, conn: _Conn, message: Message, msg_id: str | None = None) -> None:
        # A request's connection may be gone by the time a (delayed) answer is due: like the real server,
        # execution events then go to the account's current sessions.
        targets = [conn] if conn in self._conns else [c for c in self._conns if c.account_authed]
        for target in targets:
            with contextlib.suppress(Exception):
                target.writer.write(encode(message, msg_id if target is conn else None))

    def _error(
        self, conn: _Conn, msg_id: str | None, code: str, description: str = "", retry: int = 0
    ) -> None:
        err = msgs.ProtoOAErrorRes(errorCode=code, description=description)
        if retry:
            err.retryAfter = retry
        self._send(conn, err, msg_id)

    # ------------------------------------------------------------------ helpers

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _spot(self) -> msgs.ProtoOASpotEvent:
        return msgs.ProtoOASpotEvent(
            ctidTraderAccountId=self.ctid,
            symbolId=EURUSD_ID,
            bid=self.bid,
            ask=self.ask,
            timestamp=self._now_ms(),
        )

    def _trader(self) -> model.ProtoOATrader:
        return model.ProtoOATrader(
            ctidTraderAccountId=self.ctid,
            balance=self.balance_cents,
            depositAssetId=2,
            moneyDigits=2,
            leverageInCents=10_000,
            accountType=model.HEDGED,
        )

    def _exec(self, et: int, **parts: Message) -> msgs.ProtoOAExecutionEvent:
        ev = msgs.ProtoOAExecutionEvent(ctidTraderAccountId=self.ctid, executionType=et)  # type: ignore[arg-type]
        for key, value in parts.items():
            getattr(ev, key).CopyFrom(value)
        return ev

    def _pnl_cents(self, pos: model.ProtoOAPosition) -> int:
        close = self.bid if pos.tradeData.tradeSide == model.BUY else self.ask
        sign = 1 if pos.tradeData.tradeSide == model.BUY else -1
        units = pos.tradeData.volume / 100
        return round(sign * (close / 100_000 - pos.price) * units * 100)

    # ------------------------------------------------------------------ dispatch

    def _handle(self, conn: _Conn, m: Message, mid: str | None) -> None:
        now = time.monotonic()
        conn.window = [t for t in conn.window if now - t < 1.0] + [now]
        if len(conn.window) > self.rate_limit_per_s:
            self.rate_limited += 1
            self._error(conn, mid, "REQUEST_FREQUENCY_EXCEEDED", "too many requests", retry=1)
            return

        if isinstance(m, msgs.ProtoOAApplicationAuthReq):
            if (m.clientId, m.clientSecret) != (self.client_id, self.client_secret):
                self._error(conn, mid, "CH_CLIENT_AUTH_FAILURE")
                return
            conn.app_authed = True
            self._send(conn, msgs.ProtoOAApplicationAuthRes(), mid)
            return
        if not conn.app_authed:
            self._error(conn, mid, "CH_CLIENT_NOT_AUTHENTICATED")
            return
        if isinstance(m, msgs.ProtoOARefreshTokenReq):
            if m.refreshToken != self.refresh_token:
                self._error(conn, mid, "CH_ACCESS_TOKEN_INVALID")
                return
            self.refreshes += 1
            self.access_token = f"access-{self.refreshes + 1}"
            self.refresh_token = f"refresh-{self.refreshes + 1}"
            self._send(
                conn,
                msgs.ProtoOARefreshTokenRes(
                    accessToken=self.access_token,
                    tokenType="bearer",
                    expiresIn=2_628_000,
                    refreshToken=self.refresh_token,
                ),
                mid,
            )
            return
        if isinstance(m, msgs.ProtoOAGetAccountListByAccessTokenReq):
            if m.accessToken != self.access_token:
                self._error(conn, mid, "CH_ACCESS_TOKEN_INVALID")
                return
            res = msgs.ProtoOAGetAccountListByAccessTokenRes(accessToken=m.accessToken)
            res.ctidTraderAccount.add(ctidTraderAccountId=self.ctid, isLive=False, traderLogin=5550001)
            res.ctidTraderAccount.add(ctidTraderAccountId=self.ctid + 1, isLive=True, traderLogin=5550002)
            self._send(conn, res, mid)
            return
        if isinstance(m, msgs.ProtoOAAccountAuthReq):
            if m.accessToken != self.access_token or m.ctidTraderAccountId != self.ctid:
                self._error(conn, mid, "CH_ACCESS_TOKEN_INVALID")
                return
            conn.account_authed = True
            self._send(conn, msgs.ProtoOAAccountAuthRes(ctidTraderAccountId=self.ctid), mid)
            return
        if not conn.account_authed:
            self._error(conn, mid, "ACCOUNT_NOT_AUTHORIZED")
            return

        ctid = self.ctid
        if isinstance(m, msgs.ProtoOATraderReq):
            self._send(conn, msgs.ProtoOATraderRes(ctidTraderAccountId=ctid, trader=self._trader()), mid)
        elif isinstance(m, msgs.ProtoOAAssetListReq):
            res = msgs.ProtoOAAssetListRes(ctidTraderAccountId=ctid)
            res.asset.add(assetId=1, name="EUR")
            res.asset.add(assetId=2, name="USD")
            self._send(conn, res, mid)
        elif isinstance(m, msgs.ProtoOASymbolsListReq):
            res = msgs.ProtoOASymbolsListRes(ctidTraderAccountId=ctid)
            res.symbol.add(
                symbolId=EURUSD_ID, symbolName="EURUSD", enabled=True, baseAssetId=1, quoteAssetId=2
            )
            res.symbol.add(symbolId=99, symbolName="DISABLED", enabled=False, baseAssetId=1, quoteAssetId=2)
            self._send(conn, res, mid)
        elif isinstance(m, msgs.ProtoOASymbolByIdReq):
            res = msgs.ProtoOASymbolByIdRes(ctidTraderAccountId=ctid)
            if EURUSD_ID in m.symbolId:
                res.symbol.add(
                    symbolId=EURUSD_ID, digits=5, pipPosition=4, lotSize=LOT_SIZE, minVolume=STEP,
                    maxVolume=5_000_000_000, stepVolume=STEP, scheduleTimeZone="UTC",
                )  # fmt: skip
            self._send(conn, res, mid)
        elif isinstance(m, msgs.ProtoOASubscribeSpotsReq):
            conn.spots.update(m.symbolId)
            self._send(conn, msgs.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ctid), mid)
            self._send(conn, self._spot())
        elif isinstance(m, msgs.ProtoOAReconcileReq):
            res = msgs.ProtoOAReconcileRes(ctidTraderAccountId=ctid)
            res.position.extend(self.positions.values())
            res.order.extend(self.orders.values())
            self._send(conn, res, mid)
        elif isinstance(m, msgs.ProtoOAGetPositionUnrealizedPnLReq):
            res = msgs.ProtoOAGetPositionUnrealizedPnLRes(ctidTraderAccountId=ctid, moneyDigits=2)
            for pos in self.positions.values():
                pnl = self._pnl_cents(pos)
                res.positionUnrealizedPnL.add(
                    positionId=pos.positionId, grossUnrealizedPnL=pnl, netUnrealizedPnL=pnl
                )
            self._send(conn, res, mid)
        elif isinstance(m, msgs.ProtoOADealListReq):
            self.history_requests += 1
            found = [d for d in self.deals if m.fromTimestamp <= d.executionTimestamp <= m.toTimestamp]
            res = msgs.ProtoOADealListRes(ctidTraderAccountId=ctid, hasMore=len(found) > self.history_chunk)
            res.deal.extend(found[: self.history_chunk])
            self._send(conn, res, mid)
        elif isinstance(m, msgs.ProtoOAOrderListReq):
            self.history_requests += 1
            found = [o for o in self.history if m.fromTimestamp <= o.utcLastUpdateTimestamp <= m.toTimestamp]
            res = msgs.ProtoOAOrderListRes(ctidTraderAccountId=ctid, hasMore=len(found) > self.history_chunk)
            res.order.extend(found[: self.history_chunk])
            self._send(conn, res, mid)
        elif isinstance(m, msgs.ProtoOAGetTrendbarsReq):
            self._trendbars(conn, m, mid)
        elif isinstance(m, msgs.ProtoOANewOrderReq):
            self.new_order_requests.append(m.clientOrderId)
            if self.swallow_orders:  # lost on the way in: never executed, never answered
                self.swallow_orders -= 1
            elif self.ack_delay_s:
                asyncio.get_running_loop().call_later(self.ack_delay_s, self._new_order, conn, m, mid)
            else:
                self._new_order(conn, m, mid)
        elif isinstance(m, msgs.ProtoOAAmendOrderReq):
            order = self.orders.get(m.orderId)
            if order is None:
                self._send(
                    conn,
                    msgs.ProtoOAOrderErrorEvent(
                        ctidTraderAccountId=ctid, errorCode="ORDER_NOT_FOUND", orderId=m.orderId
                    ),
                    mid,
                )
                return
            if m.HasField("limitPrice"):
                order.limitPrice = m.limitPrice
            if m.HasField("stopPrice"):
                order.stopPrice = m.stopPrice
            if m.HasField("volume"):
                order.tradeData.volume = m.volume
            order.utcLastUpdateTimestamp = self._now_ms()
            self._send(conn, self._exec(model.ORDER_REPLACED, order=order), mid)
        elif isinstance(m, msgs.ProtoOACancelOrderReq):
            order = self.orders.pop(m.orderId, None)
            if order is None:
                self._send(
                    conn,
                    msgs.ProtoOAOrderErrorEvent(
                        ctidTraderAccountId=ctid, errorCode="ORDER_NOT_FOUND", orderId=m.orderId
                    ),
                    mid,
                )
                return
            order.orderStatus = model.ORDER_STATUS_CANCELLED
            self.history.append(order)
            self._send(conn, self._exec(model.ORDER_CANCELLED, order=order), mid)
        elif isinstance(m, msgs.ProtoOAAmendPositionSLTPReq):
            pos = self.positions.get(m.positionId)
            if pos is None:
                self._error(conn, mid, "POSITION_NOT_FOUND")
                return
            pos.ClearField("stopLoss")
            pos.ClearField("takeProfit")
            if m.HasField("stopLoss"):
                pos.stopLoss = m.stopLoss
            if m.HasField("takeProfit"):
                pos.takeProfit = m.takeProfit
            protection = model.ProtoOAOrder(
                orderId=next(self._ids), orderType=model.STOP_LOSS_TAKE_PROFIT, orderStatus=model.ORDER_STATUS_ACCEPTED,
                tradeData=pos.tradeData, positionId=pos.positionId,
            )  # fmt: skip
            self._send(conn, self._exec(model.ORDER_REPLACED, order=protection, position=pos), mid)
        elif isinstance(m, msgs.ProtoOAClosePositionReq):
            self._close(conn, m, mid)
        elif isinstance(m, msgs.ProtoOAAccountLogoutReq):
            self._send(conn, msgs.ProtoOAAccountLogoutRes(ctidTraderAccountId=ctid), mid)
        else:
            self._error(conn, mid, "UNSUPPORTED_MESSAGE", type(m).__name__)

    def _trendbars(self, conn: _Conn, m: msgs.ProtoOAGetTrendbarsReq, mid: str | None) -> None:
        minutes = {model.M1: 1, model.M3: 3, model.M5: 5, model.M10: 10, model.M15: 15, model.M30: 30,
                   model.H1: 60, model.H4: 240, model.D1: 1440, model.W1: 10080}.get(m.period)  # fmt: skip
        if minutes is None:
            self._error(conn, mid, "INVALID_REQUEST")
            return
        step = minutes * 60_000
        if (m.toTimestamp - m.fromTimestamp) // step > MAX_BARS_PER_REQUEST:
            self._error(conn, mid, "INCORRECT_BOUNDARIES", "period too large")
            return
        res = msgs.ProtoOAGetTrendbarsRes(ctidTraderAccountId=self.ctid, period=m.period, symbolId=m.symbolId)
        first = -(-m.fromTimestamp // step) * step
        for ms in range(first, m.toTimestamp, step):
            res.trendbar.add(volume=10, period=m.period, low=109_990, deltaOpen=5, deltaClose=8, deltaHigh=20,
                             utcTimestampInMinutes=ms // 60_000)  # fmt: skip
        self._send(conn, res, mid)

    def _new_order(self, conn: _Conn, m: msgs.ProtoOANewOrderReq, mid: str | None) -> None:
        if m.volume <= 0 or m.volume % STEP:
            self._send(
                conn,
                msgs.ProtoOAOrderErrorEvent(ctidTraderAccountId=self.ctid, errorCode="TRADING_BAD_VOLUME"),
                mid,
            )
            return
        now = self._now_ms()
        trade = model.ProtoOATradeData(
            symbolId=m.symbolId, volume=m.volume, tradeSide=m.tradeSide, openTimestamp=now, label=m.label
        )
        order = model.ProtoOAOrder(
            orderId=next(self._ids), tradeData=trade, orderType=m.orderType, orderStatus=model.ORDER_STATUS_ACCEPTED,
            clientOrderId=m.clientOrderId, utcLastUpdateTimestamp=now,
            timeInForce=m.timeInForce if m.HasField("timeInForce") else model.GOOD_TILL_CANCEL,
        )  # fmt: skip
        if m.clientOrderId:
            self.client_ids[m.clientOrderId] = order.orderId
        if m.orderType in (model.LIMIT, model.STOP, model.STOP_LIMIT):
            if m.HasField("limitPrice"):
                order.limitPrice = m.limitPrice
            if m.HasField("stopPrice"):
                order.stopPrice = m.stopPrice
            if m.HasField("stopLoss"):
                order.stopLoss = m.stopLoss
            if m.HasField("takeProfit"):
                order.takeProfit = m.takeProfit
            self.orders[order.orderId] = order
            self._send(conn, self._exec(model.ORDER_ACCEPTED, order=order), mid)
            return
        # Market: ACK (carries the clientMsgId), then the fill.
        self._send(conn, self._exec(model.ORDER_ACCEPTED, order=order), mid)
        price = (self.ask if m.tradeSide == model.BUY else self.bid) / 100_000
        pos = model.ProtoOAPosition(
            positionId=next(self._ids), tradeData=trade, positionStatus=model.POSITION_STATUS_OPEN, swap=0,
            price=price, utcLastUpdateTimestamp=now, commission=-30, moneyDigits=2, usedMargin=11_000,
        )  # fmt: skip
        sign = 1 if m.tradeSide == model.BUY else -1
        if m.HasField("relativeStopLoss"):
            pos.stopLoss = round(price - sign * m.relativeStopLoss / 100_000, 5)
        if m.HasField("relativeTakeProfit"):
            pos.takeProfit = round(price + sign * m.relativeTakeProfit / 100_000, 5)
        self.positions[pos.positionId] = pos
        order.orderStatus = model.ORDER_STATUS_FILLED
        order.executedVolume = m.volume
        order.executionPrice = price
        order.positionId = pos.positionId
        deal = model.ProtoOADeal(
            dealId=next(self._ids), orderId=order.orderId, positionId=pos.positionId, volume=m.volume,
            filledVolume=m.volume, symbolId=m.symbolId, createTimestamp=now, executionTimestamp=now,
            executionPrice=price, tradeSide=m.tradeSide, dealStatus=model.FILLED, commission=-30, moneyDigits=2,
        )  # fmt: skip
        self.balance_cents -= 30  # commission is charged on each deal
        self.deals.append(deal)
        self.history.append(order)
        self._send(conn, self._exec(model.ORDER_FILLED, order=order, position=pos, deal=deal))

    def _close(self, conn: _Conn, m: msgs.ProtoOAClosePositionReq, mid: str | None) -> None:
        pos = self.positions.get(m.positionId)
        if pos is None:
            self._error(conn, mid, "POSITION_NOT_FOUND")
            return
        if m.volume <= 0 or m.volume > pos.tradeData.volume or m.volume % STEP:
            self._send(
                conn,
                msgs.ProtoOAOrderErrorEvent(
                    ctidTraderAccountId=self.ctid, errorCode="TRADING_BAD_VOLUME", positionId=m.positionId
                ),
                mid,
            )
            return
        now = self._now_ms()
        side = model.SELL if pos.tradeData.tradeSide == model.BUY else model.BUY
        closing = model.ProtoOAOrder(
            orderId=next(self._ids), orderType=model.MARKET, orderStatus=model.ORDER_STATUS_ACCEPTED, closingOrder=True,
            positionId=pos.positionId, utcLastUpdateTimestamp=now,
            tradeData=model.ProtoOATradeData(symbolId=pos.tradeData.symbolId, volume=m.volume, tradeSide=side),
        )  # fmt: skip
        self._send(conn, self._exec(model.ORDER_ACCEPTED, order=closing, position=pos), mid)
        price = (self.bid if side == model.SELL else self.ask) / 100_000
        full_pnl = self._pnl_cents(pos)
        gross = round(full_pnl * m.volume / pos.tradeData.volume)
        self.balance_cents += gross - 30  # gross profit, minus this deal's commission
        pos.tradeData.volume -= m.volume
        if pos.tradeData.volume == 0:
            pos.positionStatus = model.POSITION_STATUS_CLOSED
            del self.positions[pos.positionId]
        closing.orderStatus = model.ORDER_STATUS_FILLED
        closing.executedVolume = m.volume
        closing.executionPrice = price
        deal = model.ProtoOADeal(
            dealId=next(self._ids), orderId=closing.orderId, positionId=pos.positionId, volume=m.volume,
            filledVolume=m.volume, symbolId=pos.tradeData.symbolId, createTimestamp=now, executionTimestamp=now,
            executionPrice=price, tradeSide=side, dealStatus=model.FILLED, commission=-30, moneyDigits=2,
            closePositionDetail=model.ProtoOAClosePositionDetail(
                entryPrice=pos.price, grossProfit=gross, swap=0, commission=-30, balance=self.balance_cents,
                closedVolume=m.volume, moneyDigits=2,
            ),
        )  # fmt: skip
        self.deals.append(deal)
        self._send(conn, self._exec(model.ORDER_FILLED, order=closing, position=pos, deal=deal))
