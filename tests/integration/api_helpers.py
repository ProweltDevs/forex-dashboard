"""A real ``App`` (paper account on the synthetic feed, the three example strategies) with the Control API
served by uvicorn on a free port, and an authenticated httpx client."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx

from tests.integration.test_app_run import synthetic_config
from trader.app import App
from trader.config.settings import AppConfig
from trader.domain.time import SystemClock
from trader.notify.notifier import Channel

TOKEN = "test-token-" + "x" * 32
ACCOUNT = "paper-main"


@dataclass
class Running:
    app: App
    client: httpx.AsyncClient
    base: str

    def price(self, symbol: str = "EURUSD") -> tuple[Decimal, Decimal]:
        tick = self.app.accounts[ACCOUNT].oms.view().last_ticks[symbol]
        return tick.bid, tick.ask


def api_config(tmp_path: Path, **extra: Any) -> AppConfig:
    raw = synthetic_config(tmp_path).model_dump(mode="json", by_alias=True)
    raw["api"] = {"enabled": True, "port": 0, "stream_tick_interval_ms": 100}
    raw.update(extra)
    return AppConfig.model_validate(raw)


@asynccontextmanager
async def running_app(
    config: AppConfig,
    *,
    env: dict[str, str] | None = None,
    channels: list[Channel] | None = None,
    configure: Callable[[App], None] | None = None,
) -> AsyncIterator[Running]:
    app = App(
        config, env={"TRADER_API_TOKEN": TOKEN, **(env or {})}, clock=SystemClock(), notify_channels=channels
    )
    if configure is not None:
        configure(app)
    await app.start()
    try:
        assert app.api is not None
        base = f"http://127.0.0.1:{app.api.port}"
        async with httpx.AsyncClient(
            base_url=base, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=10
        ) as client:
            yield Running(app, client, base)
    finally:
        await app.stop()


async def wait_for(check: Callable[[], object], timeout: float = 10.0, what: str = "") -> None:
    async with asyncio.timeout(timeout):
        while not check():
            await asyncio.sleep(0.05)
    del what
