"""The Control API (SPEC §13): FastAPI routes over ``Control``. Its OpenAPI schema is the contract for the
future UI (committed at ``docs/api/openapi.json``; a test keeps it current).

Auth: one bearer token (``TRADER_API_TOKEN``) on every route except ``GET /health``. Errors are
``{"detail": "..."}`` with 401 (token), 404 (unknown id), 409 (not possible in the current state),
422 (invalid input), 502/503 (broker error / broker unavailable).
"""

# No ``from __future__ import annotations`` here: FastAPI reads the annotations of the route functions.

import hmac
import time
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import TYPE_CHECKING, Annotated, cast

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, WebSocket
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from trader.domain.errors import (
    BrokerError,
    BrokerUnavailable,
    ConfigError,
    OrderNotFound,
    PositionNotFound,
)
from trader.observability.logging import get_logger

from . import schemas as s
from .control import Conflict, Control, NotFound
from .stream import StreamHub, serve_stream

if TYPE_CHECKING:
    from trader.app import App

_log = get_logger("trader.api")

API_VERSION = "1"


def create_api(
    control: Control, token: str, hub: StreamHub, *, stream_tick_interval_ms: int = 500
) -> FastAPI:
    api = FastAPI(
        title="Trader Control API",
        version=API_VERSION,
        description=(
            "Control API of the trading platform. Money, prices and volumes (lots) are decimal strings; "
            "times are ISO-8601 UTC. Every mutating call is audited. Manual orders go through the same "
            "risk checks as strategy orders."
        ),
    )
    bearer = HTTPBearer(auto_error=False, description="TRADER_API_TOKEN")
    expected = token.encode()

    def auth(creds: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]) -> None:
        if creds is None or not hmac.compare_digest(creds.credentials.encode(), expected):
            raise HTTPException(
                401, "invalid or missing bearer token", headers={"WWW-Authenticate": "Bearer"}
            )

    authed = [Depends(auth)]

    # ------------------------------------------------------------------ errors and request log

    def handler(status: int) -> Callable[[Request, Exception], Awaitable[JSONResponse]]:
        async def handle(_request: Request, exc: Exception) -> JSONResponse:
            return JSONResponse({"detail": str(exc) or type(exc).__name__}, status_code=status)

        return handle

    for exc_type, status in (
        (NotFound, 404),
        (OrderNotFound, 404),
        (PositionNotFound, 404),
        (Conflict, 409),
        (ValueError, 422),
        (ConfigError, 422),
        (BrokerUnavailable, 503),
        (BrokerError, 502),
    ):
        api.add_exception_handler(exc_type, handler(status))

    class RequestLog(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
            started = time.perf_counter()
            response = await call_next(request)
            log = _log.debug if request.method == "GET" else _log.info
            log(  # the path only: never the query string or headers
                "api_request",
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                ms=round((time.perf_counter() - started) * 1000, 1),
            )
            return response

    api.add_middleware(RequestLog)

    # ------------------------------------------------------------------ health, accounts

    @api.get("/health", tags=["system"])
    def health() -> s.HealthOut:
        """Unauthenticated liveness: no account details."""
        return control.health()

    @api.get("/accounts", tags=["accounts"], dependencies=authed)
    def accounts() -> list[s.AccountOut]:
        return control.accounts()

    @api.get("/accounts/{account_id}/state", tags=["accounts"], dependencies=authed)
    def account_state(account_id: str) -> s.AccountStateOut:
        return control.account_state(account_id)

    # ------------------------------------------------------------------ positions, orders

    @api.get("/positions", tags=["trading"], dependencies=authed)
    def positions(account: str | None = None, symbol: str | None = None) -> list[s.PositionOut]:
        return control.positions(account, symbol)

    @api.post("/positions/{position_id}/close", tags=["trading"], dependencies=authed)
    async def close_position(position_id: str, body: s.ClosePositionIn) -> s.CommandOut:
        """Close a position (or ``volume`` lots of it). Risk-reducing: allowed under any kill-switch level."""
        return await control.close_position(position_id, body)

    @api.get("/orders", tags=["trading"], dependencies=authed)
    def orders(
        account: str | None = None,
        open_only: Annotated[bool, Query(alias="open", description="only orders still open")] = False,
        strategy: str | None = None,
        limit: Annotated[int, Query(ge=1, le=5000)] = 200,
    ) -> list[s.OrderOut]:
        """Newest first."""
        return control.orders(account, open_only=open_only, strategy_id=strategy, limit=limit)

    @api.get("/orders/{client_order_id}", tags=["trading"], dependencies=authed)
    def order(client_order_id: str) -> s.OrderOut:
        return control.order(client_order_id)

    @api.post("/orders", tags=["trading"], dependencies=authed)
    async def submit_order(body: s.ManualOrderIn) -> s.OrderOut:
        """Manual order (``strategy_id="manual"``) through the RiskEngine and the OMS. A risk rejection is
        returned as an order with status ``REJECTED`` and its ``rejection_reason``."""
        return await control.submit_order(body)

    @api.post("/orders/{client_order_id}/cancel", tags=["trading"], dependencies=authed)
    async def cancel_order(client_order_id: str, body: s.ReasonIn) -> s.CommandOut:
        return await control.cancel_order(client_order_id, body.reason)

    # ------------------------------------------------------------------ strategies

    @api.get("/strategies", tags=["strategies"], dependencies=authed)
    def strategies() -> list[s.StrategyOut]:
        return control.strategies()

    @api.post("/strategies", tags=["strategies"], dependencies=authed)
    async def create_strategy(body: s.StrategyCreateIn) -> s.StrategyOut:
        """Load (and by default start) a strategy instance. It lasts until the process stops: add it to the
        config file to keep it."""
        return await control.create_strategy(body)

    @api.get("/strategies/{strategy_id}", tags=["strategies"], dependencies=authed)
    def strategy(strategy_id: str) -> s.StrategyOut:
        return control.strategy(strategy_id)

    @api.post("/strategies/{strategy_id}/start", tags=["strategies"], dependencies=authed)
    async def start_strategy(strategy_id: str) -> s.StrategyOut:
        """Start a loaded or stopped strategy, or resume a paused one (409 under a kill switch)."""
        return await control.start_strategy(strategy_id)

    @api.post("/strategies/{strategy_id}/pause", tags=["strategies"], dependencies=authed)
    async def pause_strategy(strategy_id: str, body: s.ReasonIn) -> s.StrategyOut:
        return await control.pause_strategy(strategy_id, body.reason)

    @api.post("/strategies/{strategy_id}/stop", tags=["strategies"], dependencies=authed)
    async def stop_strategy(strategy_id: str) -> s.StrategyOut:
        return await control.stop_strategy(strategy_id)

    @api.put("/strategies/{strategy_id}/params", tags=["strategies"], dependencies=authed)
    async def set_params(strategy_id: str, body: s.StrategyParamsIn) -> s.StrategyOut:
        """Replace the params: the old instance stops cleanly and a new one starts with its state."""
        return await control.set_params(strategy_id, body.params)

    # ------------------------------------------------------------------ risk

    @api.get("/risk", tags=["risk"], dependencies=authed)
    def risk(account: str | None = None) -> list[s.RiskAccountOut]:
        return control.risk(account)

    @api.post("/risk/kill", tags=["risk"], dependencies=authed)
    async def kill(body: s.KillIn) -> list[s.RiskAccountOut]:
        """Manual kill switch. ``account: "*"`` sets it on every account."""
        if body.account == "*":
            return await control.kill_all(body.level, body.reason)
        return [await control.kill(body.account, body.level, body.reason)]

    @api.post("/risk/reset", tags=["risk"], dependencies=authed)
    async def reset(body: s.ResetIn) -> s.RiskAccountOut:
        """Clear the manual switch and latched breakers; conditions that still hold trigger again."""
        return await control.reset(body.account, body.reason)

    # ------------------------------------------------------------------ P&L, trades, analytics, audit

    @api.get("/pnl", tags=["analytics"], dependencies=authed)
    def pnl(account: str, day: date | None = None) -> s.PnlOut:
        """Realized P&L of a trading day (default today) by strategy and symbol, plus current equity."""
        return control.pnl(account, day)

    @api.get("/trades", tags=["analytics"], dependencies=authed)
    def trades(
        account: str | None = None,
        strategy: str | None = None,
        symbol: str | None = None,
        since: datetime | None = None,
        limit: Annotated[int, Query(ge=1, le=10000)] = 200,
    ) -> list[s.TradeOut]:
        """Closed trades, newest first."""
        return control.trades(account, strategy_id=strategy, symbol=symbol, since=since, limit=limit)

    @api.post("/trades/{trade_id}/notes", tags=["analytics"], dependencies=authed)
    async def add_note(trade_id: Annotated[str, Path(min_length=1)], body: s.NoteIn) -> s.CommandOut:
        return await control.add_note(trade_id, body.account, body.text)

    @api.get("/analytics/execution", tags=["analytics"], dependencies=authed)
    def execution_stats(
        account: str | None = None,
        by: Annotated[
            str, Query(description=f"comma-separated: {', '.join(Control.EXECUTION_KEYS)}")
        ] = "symbol",
    ) -> list[s.StatsOut]:
        """Execution quality (slippage, latency, fill and requote rates) grouped by ``by``."""
        return control.execution(account, _keys(by))

    @api.get("/analytics/performance", tags=["analytics"], dependencies=authed)
    def performance(
        account: str | None = None,
        by: Annotated[
            str, Query(description=f"comma-separated: {', '.join(Control.PERFORMANCE_KEYS)}")
        ] = "strategy",
        since: datetime | None = None,
    ) -> list[s.StatsOut]:
        """Trade metrics (net, win rate, profit factor, expectancy, drawdown, ...) grouped by ``by``."""
        return control.performance(account, _keys(by), since)

    @api.get("/audit", tags=["system"], dependencies=authed)
    async def audit(
        limit: Annotated[int, Query(ge=1, le=10000)] = 100,
        action: str | None = None,
        account: str | None = None,
    ) -> list[s.AuditOut]:
        """Manual commands, newest first."""
        return await control.audit(limit=limit, action=action, account_id=account)

    # ------------------------------------------------------------------ stream

    @api.websocket("/stream")
    async def stream(ws: WebSocket) -> None:
        await serve_stream(ws, hub, token, stream_tick_interval_ms)

    return api


def openapi_schema() -> dict[str, object]:
    """The API contract, without a running app (``trader openapi``)."""
    from trader.marketdata.bus import Bus, TickBus

    api = create_api(Control(cast("App", None)), "x" * 32, StreamHub([Bus()], TickBus()))
    return api.openapi()


def _keys(by: str) -> tuple[str, ...]:
    keys = tuple(k.strip() for k in by.split(",") if k.strip())
    if not keys:
        raise ValueError("by needs at least one key")
    return keys
