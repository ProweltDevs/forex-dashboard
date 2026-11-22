"""Ops endpoints (SPEC §15) on their own port (``ops:``), no auth, for Prometheus and health checks:

- ``GET /metrics``: Prometheus text format.
- ``GET /health/live``: 200 while the process serves requests (the event loop is running). The Docker
  health check uses it: restarting a hung process is safe because startup always reconciles first.
- ``GET /health`` (also ``/health/ready``): per-account connection status. 200 when every account is
  connected and reconciled (``ready``, or ``degraded`` when something needs a look: a stale feed, the
  database down, a kill switch on), 503 while ``starting``, ``not_ready`` or ``stopping``.

The ops server starts before anything else, so liveness answers during a long startup, and stops last.
Bind it to localhost or a private network: it shows account status without asking who is looking.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from trader import __version__
from trader.config.settings import OpsConfig
from trader.domain.enums import KillLevel

from .http import EmbeddedServer
from .logging import get_logger

if TYPE_CHECKING:
    from trader.app import App

_log = get_logger("trader.ops")


def health(app: App) -> tuple[int, dict[str, Any]]:
    """(HTTP status, body)."""
    accounts: dict[str, Any] = {}
    not_ready: list[str] = []
    notes: list[str] = []
    for aid, s in app.accounts.items():
        view = s.oms.view()
        ready = s.oms.ready.is_set()
        level = s.oms.risk_level
        accounts[aid] = {
            "broker": s.account.broker,
            "environment": s.account.environment,
            "connected": view.broker_connected,
            "ready": ready,
            "kill_level": str(level),
            "positions": len(view.positions),
            "open_orders": len(view.open_orders),
        }
        if not (view.broker_connected and ready):
            not_ready.append(aid)
        if level is not KillLevel.NONE:
            notes.append(f"{aid}: kill switch {level}")
    stale = sorted(f"{feed}:{symbol}" for feed, symbol in app.monitor.stale)
    if stale:
        notes.append(f"stale feeds: {', '.join(stale)}")
    db: dict[str, Any] = {"configured": app.persistence is not None}
    if app.persistence is not None:
        writer = app.persistence.writer
        db |= {"up": not writer.down, "pending": writer.pending}
        if writer.down:
            notes.append("database down (buffering to the WAL)")
    strategies: dict[str, int] = {}
    if app.runner is not None:
        for _sid, st, _reason, _stats in app.runner.status():
            strategies[str(st)] = strategies.get(str(st), 0) + 1
    status: str
    if app.state != "running":
        status = app.state  # starting / stopping / stopped / created
    elif not_ready or not accounts:
        status = "not_ready"
    elif notes:
        status = "degraded"
    else:
        status = "ready"
    body = {
        "status": status,
        "mode": str(app.config.mode),
        "version": __version__,
        "uptime_s": round((app.clock.now() - app.created).total_seconds(), 1),
        "accounts": accounts,
        "not_ready": not_ready,
        "notes": notes,
        "database": db,
        "strategies": strategies,
        "clock_offset_s": {k: round(v, 3) for k, v in app.monitor.clock_offsets().items()},
    }
    return (200 if status in ("ready", "degraded") else 503), body


def create_ops_app(app: App) -> FastAPI:
    ops = FastAPI(title="trader ops", docs_url=None, redoc_url=None, openapi_url=None)

    @ops.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(app.metrics.registry), media_type=CONTENT_TYPE_LATEST)

    @ops.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "alive"}

    @ops.get("/health")
    @ops.get("/health/ready")
    def ready() -> JSONResponse:
        status, body = health(app)
        return JSONResponse(body, status_code=status)

    return ops


class OpsServer:
    def __init__(self, app: App, config: OpsConfig) -> None:
        self._server = EmbeddedServer(create_ops_app(app), host=config.host, port=config.port, name="ops")

    @property
    def port(self) -> int:
        return self._server.port

    async def start(self) -> None:
        await self._server.start()
        _log.info("ops_started", host=self._server.host, port=self.port)

    async def stop(self) -> None:
        await self._server.stop()
