from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from .harness import HARNESSES, ContractHarness

# Real adapters run only with `-m demo` against DEMO accounts; they skip when credentials are missing.
ADAPTERS = [
    pytest.param("paper", id="paper"),
    pytest.param("ctrader-fake", id="ctrader-fake"),
    pytest.param("ctrader-demo", id="ctrader-demo", marks=pytest.mark.demo),
]


@pytest.fixture(params=ADAPTERS)
async def harness(request: pytest.FixtureRequest) -> AsyncIterator[ContractHarness]:
    h = await HARNESSES[request.param]()
    try:
        yield h
    finally:
        await h.cleanup()
