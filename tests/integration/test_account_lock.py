"""One writer per account (CLAUDE.md rule 3): the OMS state file is locked for as long as an OMS owns it.
A second ``trader run``, or ``trader kill`` while ``trader run`` manages the account, is refused instead of
silently overwriting the other's state, kill switch included."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.integration.api_helpers import api_config
from trader.app import App
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
    config = api_config(tmp_path, api={"enabled": False})
    app = App(config, env={}, clock=SystemClock())
    await app.start()
    try:
        snapshot = (tmp_path / "oms" / "paper-main.json").read_bytes()
        intruder = App(config, env={}, clock=SystemClock())
        with pytest.raises(ConfigError, match=r"another process .* manages this account"):
            await intruder.start()
        await intruder.stop()
        assert app.accounts["paper-main"].oms.ready.is_set()  # the owner trades on
        assert (tmp_path / "oms" / "paper-main.json").read_bytes() == snapshot  # untouched by the intruder
    finally:
        await app.stop()
    again = App(config, env={}, clock=SystemClock())  # the lock went with the first app
    await again.start()
    await again.stop()
