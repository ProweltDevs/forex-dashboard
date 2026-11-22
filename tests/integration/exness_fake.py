"""In-process fake of the Exness Trader API (spec v1.0.2) for tests.

REST is served through ``httpx.MockTransport`` (no sockets); WebSockets run on a local ``websockets``
server. Every REST request and WebSocket handshake is signature-verified with the account's Ed25519
public key exactly as the docs describe, so signing bugs fail tests. Models: discovery, limits, ACK ->
transaction_event, idempotency keys, sltp orders, partial close, snapshot, candles with a per-request
limit, and per-operation rate limits. One account, one instrument (EURUSD), hedging, fills at the touch.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import itertools
import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import httpx
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.datastructures import Headers
from websockets.http11 import Request, Response

Json = dict[str, Any]
D = Decimal
ACCOUNT = "70001234"
API_KEY = "exnsk-test-key"
MAX_CANDLES = 1000


def _b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass
class _Client:
    ws: ServerConnection
    kind: str  # events | ticks
    subs: set[str] = field(default_factory=set)
    tick_instruments: set[str] = field(default_factory=set)


class FakeExness:
    def __init__(self, *, rate_limit_per_s: int = 50, balance: str = "10000.00") -> None:
        key = Ed25519PrivateKey.generate()
        self.private_key_hex = (
            key.private_bytes(
                serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption()
            )
            + key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        ).hex()
        self._public: Ed25519PublicKey = key.public_key()
        self.rate_limit_per_s = rate_limit_per_s
        self.balance = D(balance)
        self.bid, self.ask = D("1.10000"), D("1.10010")
        self.positions: dict[str, Json] = {}
        self.orders: dict[str, Json] = {}  # active limit/stop + sltp
        self.history: list[
            Json
        ] = []  # closed orders (filled/cancelled/rejected), for GET /history/.../orders
        self.deals: list[Json] = []  # for GET /history/.../deals
        self.operations: dict[str, Json] = {}
        self.idempotency: dict[tuple[str, str], tuple[str, str]] = {}  # (key, op) -> (body hash, op id)
        self.requests: list[str] = []  # "METHOD path" of every REST call (signature-verified)
        self.auth_failures = 0
        self.rate_limited = 0
        self.drop_next_transaction = False  # simulate a missed transaction_event
        self._ids = itertools.count(1000)
        self._window: dict[str, list[float]] = {}
        self._clients: list[_Client] = []
        self._server: Server | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self.port = 0
        self.transport = httpx.MockTransport(self._handle)

    # ------------------------------------------------------------------ lifecycle

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def start(self) -> None:
        self._server = await serve(self._ws_handler, "127.0.0.1", 0, process_request=self._ws_auth)
        self.port = self._server.sockets[0].getsockname()[1]
        self._tasks = [asyncio.create_task(self._ticker()), asyncio.create_task(self._account_ticker())]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await self.drop_all()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def drop_all(self) -> None:
        for c in list(self._clients):
            with contextlib.suppress(Exception):
                await c.ws.close()
        self._clients.clear()

    # ------------------------------------------------------------------ auth

    def _verify(self, method: str, path_q: str, headers: Any, body: bytes) -> str | None:
        """Return an error_message if the signed request is invalid, else None."""
        try:
            data_raw = _b64decode(headers["EXN-DATA"])
            sig = _b64decode(headers["EXN-SIGN"])
            data = json.loads(data_raw)
            self._public.verify(sig, data_raw)
        except (KeyError, ValueError, InvalidSignature):
            return "AUTH_INVALID_SIGNATURE"
        checks = [
            headers.get("EXN-API-KEY") == API_KEY == data.get("api_key"),
            headers.get("EXN-SIGN-VERSION") == "1" and data.get("sign_version") == 1,
            headers.get("EXN-TIMESTAMP") == str(data.get("timestamp")),
            abs(time.time() * 1000 - int(data.get("timestamp", 0))) < 30_000,
            data.get("method") == method,
            data.get("path") == path_q,
            data.get("body_hash")
            == base64.urlsafe_b64encode(hashlib.sha256(body).digest()).decode().rstrip("="),
            "EXN-IDEMPOTENCY-KEY" in headers
            and headers["EXN-IDEMPOTENCY-KEY"] == data.get("idempotency_key"),
            (method == "GET") == (data.get("idempotency_key") == ""),
        ]
        if not all(checks):
            return "AUTH_INVALID_API_KEY" if data.get("api_key") != API_KEY else "AUTH_INVALID_SIGNATURE"
        return None

    def _ws_auth(self, connection: ServerConnection, request: Request) -> Response | None:
        error = self._verify("GET", request.path, request.headers, b"")
        if error:
            self.auth_failures += 1
            return connection.respond(401, json.dumps({"code": 1001, "error_message": error}))
        return None

    # ------------------------------------------------------------------ REST

    @staticmethod
    def _json(status: int, data: Json) -> httpx.Response:
        return httpx.Response(status, json=data)

    def _err(self, status: int, code: int, name: str) -> httpx.Response:
        return self._json(status, {"code": code, "error_message": name})

    async def _handle(self, request: httpx.Request) -> httpx.Response:
        path_q = request.url.raw_path.decode()
        method = request.method
        body = request.content
        error = self._verify(method, path_q, request.headers, body)
        if error:
            self.auth_failures += 1
            return self._err(401, 1001, error)
        path = urlsplit(path_q).path
        query = parse_qsl(urlsplit(path_q).query)
        if len({k for k, _ in query}) != len(query):
            return self._err(400, 3001, "REQUEST_INVALID_PARAMETERS")
        q = dict(query)
        self.requests.append(f"{method} {path}")
        now = time.monotonic()
        win = [t for t in self._window.get(path, []) if now - t < 1.0] + [now]
        self._window[path] = win
        if len(win) > self.rate_limit_per_s:
            self.rate_limited += 1
            return self._err(429, 3015, "REQUEST_RATE_LIMIT")
        acct = f"/accounts/{ACCOUNT}"
        payload: Json = json.loads(body) if body else {}
        key = request.headers.get("EXN-IDEMPOTENCY-KEY", "")

        if path == "/v1/trading/access-point":
            return self._json(200, {"access_point": self.base, "account_id": q.get("account_id")})
        if acct not in path:
            return self._err(404, 2000, "ACCOUNT_NOT_FOUND")
        if method == "GET" and path.endswith("/account"):
            return self._json(200, {"account_id": ACCOUNT, "account_name": "Demo test", "create_date": "2026-01-01T00:00:00+00:00",
                                    "settings": {"currency": "USD", "leverage": "200", "account_status": "active",
                                                 "trade_mode": "enabled", "execution_mode": "market"}})  # fmt: skip
        if method == "GET" and path.endswith("/limits"):
            return self._json(200, self._limits())
        if method == "GET" and path.endswith("/instruments"):
            return self._json(200, {"instruments": ["EURUSDm", "BTCUSDm"]})
        if method == "GET" and path.endswith("/conditions"):
            name = path.split("/")[-2]
            if name != "EURUSDm":
                return self._err(404, 4000, "MARKET_INSTRUMENT_NOT_FOUND")
            return self._json(200, self._conditions())
        if method == "GET" and path.endswith("/snapshot"):
            return self._json(200, self._snapshot())
        if method == "GET" and path.endswith("/candles"):
            return self._candles(q)
        if method == "GET" and "/history/accounts/" in path:
            return self._history(path.rsplit("/", 1)[-1], q)
        if method == "GET" and "/operations/" in path:
            op = self.operations.get(path.rsplit("/", 1)[-1])
            return self._json(200, op) if op else self._err(404, 3014, "REQUEST_OPERATION_NOT_FOUND")

        # ---- mutating: idempotency first
        op_kind = f"{method} {path.rsplit('/', 1)[0] if method != 'POST' else path}"
        body_hash = hashlib.sha256(body + path_q.encode()).hexdigest()
        known = self.idempotency.get((key, op_kind))
        if known is not None:
            if known[0] != body_hash:
                return self._err(409, 3000, "REQUEST_INVALID")
            return self._json(202, {"operation_id": known[1], "client_request_id": key, "status": "accepted"})
        op_id = str(next(self._ids))
        self.idempotency[(key, op_kind)] = (body_hash, op_id)
        self.operations[op_id] = {"operation_id": op_id, "status": "pending", "client_request_id": key,
                                  "created_at": _now_iso(), "updated_at": _now_iso()}  # fmt: skip

        if method == "POST" and path.endswith("/positions"):
            action = lambda: self._open(payload)  # noqa: E731
        elif method == "POST" and path.endswith("/orders"):
            action = lambda: self._place(payload)  # noqa: E731
        elif method == "PATCH" and "/orders/" in path:
            oid = path.rsplit("/", 1)[-1]
            if oid not in self.orders:
                return self._err(404, 3012, "REQUEST_ORDER_NOT_FOUND")
            action = lambda: self._modify(oid, payload)  # noqa: E731
        elif method == "DELETE" and "/orders/" in path:
            oid = path.rsplit("/", 1)[-1]
            if oid not in self.orders:
                return self._err(404, 3012, "REQUEST_ORDER_NOT_FOUND")
            action = lambda: self._cancel(oid)  # noqa: E731
        elif method == "DELETE" and "/positions/" in path:
            pid = path.rsplit("/", 1)[-1]
            if pid not in self.positions:
                return self._err(404, 3013, "REQUEST_POSITION_NOT_FOUND")
            action = lambda: self._close(pid, q.get("volume"))  # noqa: E731
        else:
            return self._err(405, 3011, "REQUEST_METHOD_NOT_ALLOWED")
        self._tasks.append(asyncio.create_task(self._execute(op_id, key, action)))
        return self._json(202, {"operation_id": op_id, "client_request_id": key, "status": "accepted"})

    async def _execute(self, op_id: str, key: str, action: Any) -> None:
        await asyncio.sleep(0.01)  # ACK first, result later
        result: Json | tuple[int, str] = action()
        op = self.operations[op_id]
        event: Json = {"event_type": "transaction_event", "operation_id": op_id, "client_request_id": key,
                       "event_time": _now_iso()}  # fmt: skip
        if isinstance(result, tuple):
            op.update(status="rejected", error_code=result[0], error_message=result[1])
            event.update(status="rejected", error_code=result[0], error_message=result[1], payload={})
        else:
            op.update(status="confirmed")
            event.update(status="success", payload=result)
        op["updated_at"] = _now_iso()
        if self.drop_next_transaction:
            self.drop_next_transaction = False
            return
        await self._broadcast("events", "transactions", event)

    # ------------------------------------------------------------------ trading logic

    def _account_state(self) -> Json:
        pnl = sum((self._pnl(p) for p in self.positions.values()), D(0))
        margin = sum((D(p["volume"]) * 100_000 / 200 * D("1.1") for p in self.positions.values()), D(0))
        return {"balance": f"{self.balance:.2f}", "equity": f"{self.balance + pnl:.2f}",
                "used_margin": f"{margin:.2f}", "leverage": "200"}  # fmt: skip

    def _pnl(self, p: Json) -> D:
        close = self.bid if p["direction"] == "buy" else self.ask
        sign = 1 if p["direction"] == "buy" else -1
        return (sign * (close - D(p["open_price"])) * D(p["volume"]) * 100_000).quantize(D("0.01"))

    def _valid_volume(self, v: str | None) -> bool:
        try:
            vol = D(v or "0")
        except ArithmeticError:
            return False
        return D("0.01") <= vol <= D(200) and vol % D("0.01") == 0

    def _open(self, req: Json) -> Json | tuple[int, str]:
        if req.get("instrument") != "EURUSDm":
            return (4000, "MARKET_INSTRUMENT_NOT_FOUND")
        if not self._valid_volume(req.get("volume")):
            return (5003, "TRADING_RULE_INVALID_VOLUME_STEP")
        price = self.ask if req["side"] == "buy" else self.bid
        oid, pid, did = (str(next(self._ids)) for _ in range(3))
        now = _now_iso()
        pos = {"position_id": pid, "direction": req["side"], "instrument": "EURUSDm", "initial_volume": req["volume"],
               "volume": req["volume"], "open_price": f"{price}", "state": "open", "margin_rate": "1.1",
               "create_time": now, "commission": "-0.30", "swap": "0"}  # fmt: skip
        self.positions[pid] = pos
        self.balance -= D("0.30")
        orders: list[Json] = [{"order_id": oid, "position_id": pid, "order_type": "market", "state": "filled",
                               "direction": req["side"], "instrument": "EURUSDm", "price": f"{price}",
                               "volume": req["volume"], "create_time": now}]  # fmt: skip
        self.history.append(dict(orders[0]))
        if req.get("stop_loss_price") or req.get("take_profit_price"):
            orders.append(self._new_sltp(pid, req.get("stop_loss_price"), req.get("take_profit_price")))
        deal = {"deal_id": did, "order_id": oid, "position_id": pid, "type": "open", "direction": req["side"],
                "instrument": "EURUSDm", "volume": req["volume"], "price": f"{price}", "create_time": now,
                "commission": "-0.30"}  # fmt: skip
        self.deals.append(dict(deal))
        return {"orders": orders, "positions": [pos], "deals": [deal], "account_state": self._account_state()}

    def _new_sltp(self, pid: str, sl: str | None, tp: str | None) -> Json:
        o = {"order_id": str(next(self._ids)), "position_id": pid, "order_type": "sltp", "state": "placed",
             "stop_loss_price": sl, "take_profit_price": tp, "create_time": _now_iso()}  # fmt: skip
        self.orders[o["order_id"]] = o
        return o

    def _place(self, req: Json) -> Json | tuple[int, str]:
        if req.get("type") == "sltp":
            pid = str(req.get("position_id"))
            if pid not in self.positions:
                return (3013, "REQUEST_POSITION_NOT_FOUND")
            return {"orders": [self._new_sltp(pid, req.get("stop_loss_price"), req.get("take_profit_price"))],
                    "account_state": self._account_state()}  # fmt: skip
        if not self._valid_volume(req.get("volume")):
            return (5003, "TRADING_RULE_INVALID_VOLUME_STEP")
        price = D(req["price"])
        buy = req["side"] == "buy"
        wrong_side = (price >= self.ask) if (req["type"] == "limit") == buy else (price <= self.bid)
        if wrong_side:
            return (5005, "TRADING_RULE_INVALID_PRICE_LEVELS")
        o = {"order_id": str(next(self._ids)), "order_type": req["type"], "state": "placed", "direction": req["side"],
             "instrument": "EURUSDm", "price": req["price"], "volume": req["volume"],
             "stop_loss_price": req.get("stop_loss_price"), "take_profit_price": req.get("take_profit_price"),
             "create_time": _now_iso()}  # fmt: skip
        self.orders[o["order_id"]] = o
        return {"orders": [o], "account_state": self._account_state()}

    def _modify(self, oid: str, req: Json) -> Json:
        o = self.orders[oid]
        for k in ("price", "stop_loss_price", "take_profit_price"):
            if k in req:
                o[k] = req[k]
        o["update_time"] = _now_iso()
        if o["order_type"] == "sltp" and not o.get("stop_loss_price") and not o.get("take_profit_price"):
            o["state"] = "cancelled"
            del self.orders[oid]
        return {"orders": [dict(o)], "account_state": self._account_state()}

    def _cancel(self, oid: str) -> Json:
        o = self.orders.pop(oid)
        o.update(state="cancelled", update_time=_now_iso())
        self.history.append(dict(o))
        return {"orders": [o], "account_state": self._account_state()}

    def _close(self, pid: str, volume: str | None) -> Json | tuple[int, str]:
        pos = self.positions[pid]
        vol = D(volume) if volume else D(pos["volume"])
        if vol > D(pos["volume"]) or not self._valid_volume(str(vol)):
            return (5004, "TRADING_RULE_INVALID_REMAINING_VOLUME")
        side = "sell" if pos["direction"] == "buy" else "buy"
        price = self.bid if side == "sell" else self.ask
        profit = (self._pnl(pos) * vol / D(pos["volume"])).quantize(D("0.01"))
        self.balance += profit - D("0.30")
        now = _now_iso()
        oid, did = str(next(self._ids)), str(next(self._ids))
        remaining = D(pos["volume"]) - vol
        pos = {**pos, "volume": f"{remaining}"}
        if remaining == 0:
            pos["state"] = "closed"
            del self.positions[pid]
            for k in [k for k, o in self.orders.items() if o.get("position_id") == pid]:
                del self.orders[k]
        else:
            self.positions[pid] = pos
        order = {"order_id": oid, "position_id": pid, "order_type": "market", "state": "filled", "direction": side,
                 "instrument": "EURUSDm", "price": f"{price}", "volume": f"{vol}", "create_time": now}  # fmt: skip
        deal = {"deal_id": did, "order_id": oid, "position_id": pid, "type": "close", "direction": side,
                "instrument": "EURUSDm", "volume": f"{vol}", "price": f"{price}", "create_time": now,
                "commission": "-0.30", "profit": {"amount": f"{profit}", "currency": "USD"}}  # fmt: skip
        self.history.append(dict(order))
        self.deals.append(dict(deal))
        return {
            "orders": [order],
            "positions": [pos],
            "deals": [deal],
            "account_state": self._account_state(),
        }

    # ------------------------------------------------------------------ reads

    def _history(self, kind: str, q: dict[str, str]) -> httpx.Response:
        """Closed orders / deals by create_time in [from, to), one page (enough for tests)."""
        if "from" not in q:
            return self._err(400, 3001, "REQUEST_INVALID_PARAMETERS")
        lo = datetime.fromisoformat(q["from"].replace("Z", "+00:00"))
        hi = datetime.fromisoformat(q["to"].replace("Z", "+00:00")) if "to" in q else datetime.now(UTC)
        rows = self.history if kind == "orders" else self.deals
        item = "order" if kind == "orders" else "deal"
        items = [
            {"event_time": r["create_time"], item: r}
            for r in rows
            if lo <= datetime.fromisoformat(r["create_time"]) < hi
            and r.get("instrument") == q.get("instrument", r.get("instrument"))
            and r.get("type") == q.get("deal_type", r.get("type"))
        ]
        return self._json(200, {"items": items, "has_more": False})

    def _snapshot(self) -> Json:
        return {"orders": list(self.orders.values()), "positions": list(self.positions.values()),
                "account_state": self._account_state()}  # fmt: skip

    def _limits(self) -> Json:
        ops = ["getCandleHistory", "placePendingOrder", "modifyOrder", "cancelPendingOrder", "openPosition",
               "closePosition", "getTradingStateSnapshot", "getOperationStatus", "getAccountDetails",
               "getAvailableInstrumentList", "getInstrumentCondition", "getLimits"]  # fmt: skip
        methods = []
        for op in ops:
            entry: Json = {
                "api_operation_id": op,
                "http_method": "GET",
                "path": "/",
                "rate_limit": {"limit": 20, "window_seconds": 1},
            }
            if op == "getCandleHistory":
                entry["limits"] = {"max_candles_per_request": MAX_CANDLES}
            methods.append(entry)
        return {"limits": {"rest": {"global_account_rate": {"limit": self.rate_limit_per_s - 5, "window_seconds": 1},
                                    "methods": methods},
                           "websocket": {"global_account_limits": {}, "endpoints": []}}}  # fmt: skip

    def _conditions(self) -> Json:
        return {"instrument": "EURUSDm", "base_currency": "EUR", "quote_currency": "USD", "margin_currency": "EUR",
                "point_digits": 5, "calc_mode": "forex", "exec_mode": "market", "contract_size": "100000",
                "volume_min": "0.01", "volume_max": "200", "volume_step": "0.01", "trade_mode": "enabled",
                "order_types": ["market", "limit", "stop"],
                "trade_sessions": [{"trade_mode": "enabled", "week_start_time": "Mon, 00:00:00",
                                    "week_end_time": "Sun, 23:59:59"}]}  # fmt: skip

    def _candles(self, q: dict[str, str]) -> httpx.Response:
        minutes = {"M1": 1, "M3": 3, "M5": 5, "M10": 10, "M15": 15, "M30": 30, "H1": 60, "H4": 240, "D1": 1440,
                   "W1": 10080, "S1": 1 / 60, "S5": 5 / 60, "S15": 15 / 60}.get(q.get("timeframe", ""))  # fmt: skip
        if minutes is None:
            return self._err(400, 3003, "REQUEST_INVALID_TIMEFRAME")
        start = datetime.fromisoformat(q["from"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(q["to"].replace("Z", "+00:00"))
        step = timedelta(minutes=minutes)
        if (end - start) / step > MAX_CANDLES:
            return self._err(400, 3004, "REQUEST_INVALID_TIME_RANGE")
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        t = epoch + -((epoch - start) // step) * step
        out = []
        while t < end:
            out.append({"time": t.isoformat(), "open": 1.09995, "high": 1.1001, "low": 1.0999, "close": 1.09998,
                        "volume": 12.0})  # fmt: skip
            t += step
        return self._json(200, {"candles": out})

    # ------------------------------------------------------------------ WebSockets

    async def _ws_handler(self, ws: ServerConnection) -> None:
        kind = "ticks" if ws.request is not None and ws.request.path.endswith("/ws/ticks") else "events"
        client = _Client(ws, kind)
        self._clients.append(client)
        try:
            async for raw in ws:
                msg = json.loads(raw)
                sub = msg.get("subscribe") or {}
                event = sub.get("event")
                if kind == "ticks":
                    if event != "ticks" or not sub.get("instruments"):
                        await ws.send(
                            json.dumps(
                                {"id": msg.get("id"), "code": 3000, "error_message": "REQUEST_INVALID"}
                            )
                        )
                        continue
                    client.tick_instruments.update(sub["instruments"])
                    await ws.send(json.dumps(self._tick()))
                elif event in ("transactions", "account_state"):
                    client.subs.add(event)
                    if event == "transactions":
                        await ws.send(
                            json.dumps({"event_type": "trading_state_snapshot", "payload": self._snapshot()})
                        )
                else:
                    await ws.send(
                        json.dumps({"id": msg.get("id"), "code": 3000, "error_message": "REQUEST_INVALID"})
                    )
        except Exception:
            pass
        finally:
            if client in self._clients:
                self._clients.remove(client)

    def _tick(self) -> Json:
        return {
            "instrument": "EURUSDm",
            "bid": float(self.bid),
            "ask": float(self.ask),
            "timestamp": _now_iso(),
        }

    async def _broadcast(self, kind: str, sub: str, message: Json) -> None:
        text = json.dumps(message)
        for c in list(self._clients):
            if c.kind == kind and (sub in c.subs or (kind == "ticks" and "EURUSDm" in c.tick_instruments)):
                with contextlib.suppress(Exception):
                    await c.ws.send(text)

    async def _ticker(self) -> None:
        i = 0
        while True:
            await asyncio.sleep(0.02)
            i += 1
            self.bid = D("1.10000") + D("0.00002") * (i % 2)
            self.ask = self.bid + D("0.00010")
            await self._broadcast("ticks", "ticks", self._tick())

    async def _account_ticker(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            await self._broadcast("events", "account_state", {"event_type": "account_state_event",
                                  "event_time": _now_iso(), "payload": {"account_state": self._account_state()}})  # fmt: skip


__all__ = ["ACCOUNT", "API_KEY", "FakeExness", "Headers"]
