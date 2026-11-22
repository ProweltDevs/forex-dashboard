from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from tests.chaos.harness import Trader
from tests.integration.ctrader_fake import FakeCTraderServer


@pytest.fixture
async def broker() -> AsyncIterator[FakeCTraderServer]:
    server = FakeCTraderServer()
    await server.start()
    yield server
    await server.stop()


@pytest.fixture
async def trader(broker: FakeCTraderServer, tmp_path: Path) -> AsyncIterator[Trader]:
    t = Trader(tmp_path, broker)
    yield t
    await t.cleanup()
