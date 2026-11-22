from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from .harness import HARNESSES, ContractHarness

# Real adapters are added here as pytest.param("ctrader", marks=pytest.mark.demo) in phases 2-3.
ADAPTERS = [pytest.param("paper", id="paper")]


@pytest.fixture(params=ADAPTERS)
async def harness(request: pytest.FixtureRequest) -> AsyncIterator[ContractHarness]:
    h = await HARNESSES[request.param]()
    try:
        yield h
    finally:
        await h.cleanup()
