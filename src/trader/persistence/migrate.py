"""Run the Alembic migrations programmatically (``trader run`` does it on the first successful database
connection). Blocking (Alembic's async env runs its own event loop): call it from a worker thread."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

MIGRATIONS = Path(__file__).parent / "migrations"


def _config(url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS))
    cfg.attributes["url"] = url
    cfg.attributes["configure_logging"] = False
    return cfg


def upgrade(url: str, revision: str = "head") -> None:
    command.upgrade(_config(url), revision)


def head() -> str:
    rev = ScriptDirectory.from_config(_config("unused")).get_current_head()
    assert rev is not None
    return rev
