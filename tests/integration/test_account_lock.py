"""One writer per account (CLAUDE.md rule 3): the OMS state file is locked for as long as an OMS owns it.
A second ``trader run``, or ``trader kill`` while ``trader run`` manages the account, is refused instead of
silently overwriting the other's state, kill switch included."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.integration.api_helpers import api_config, wait_for
from trader.app import App
from trader.domain.enums import KillLevel
from trader.domain.errors import ConfigError
from trader.domain.time import SystemClock
from trader.oms import AccountLocked, JsonFileStore


def test_the_state_file_has_one_owner(tmp_path: Path) -> None:
    first, second = JsonFileStore(tmp_path / "acct.json"), JsonFileStore(tmp_path / "acct.json")
    first.open()
    with pytest.raises(AccountLocked, match=f"pid {os.getpid()}"):
        second.open()
    first.close()
    second.open()  # released: the next owner takes it
    second.close()


async def test_a_second_app_on_the_same_data_dir_is_refused(tmp_path: Path) -> None:
    # No strategies: nothing but this test changes the owner's state, so the file content is predictable.
    config = api_config(tmp_path, api={"enabled": False}, strategies=[])
    snapshot = tmp_path / "oms" / "paper-main.json"
    app = App(config, env={}, clock=SystemClock())
    await app.start()
    try:
        oms = app.accounts["paper-main"].oms
        await oms.kill_switch(KillLevel.PAUSE_NEW, "owner state")  # the owner persists it
        await wait_for(lambda: snapshot.exists() and "PAUSE_NEW" in snapshot.read_text(), what="snapshot")
        before = snapshot.read_bytes()

        intruder = App(config, env={}, clock=SystemClock())
        with pytest.raises(ConfigError, match=r"another process .* manages this account"):
            await intruder.start()
        await intruder.stop()
        # A refused start never writes: had the intruder saved its empty state, the kill switch would be gone.
        assert snapshot.read_bytes() == before
        assert oms.ready.is_set()  # the owner trades on
        assert oms.risk_level is KillLevel.PAUSE_NEW
    finally:
        await app.stop()
    again = App(config, env={}, clock=SystemClock())  # the lock went with the first app
    await again.start()
    try:
        assert (
            again.accounts["paper-main"].oms.risk_level is KillLevel.PAUSE_NEW
        )  # the owner's state, restored
    finally:
        await again.stop()
