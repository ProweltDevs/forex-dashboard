"""Shared fixtures: a throwaway PostgreSQL server per test session (fresh database per test)."""

from __future__ import annotations

import itertools
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.pg import TempPostgres

_dbs = itertools.count(1)


@pytest.fixture(scope="session")
def pg_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[TempPostgres]:
    server = TempPostgres(tmp_path_factory.mktemp("pg")).init()
    yield server
    server.destroy()


@pytest.fixture
def pg_url(pg_server: TempPostgres) -> str:
    """A fresh, empty database on the session server."""
    name = f"t{next(_dbs)}"
    pg_server._run("createdb", "-h", "127.0.0.1", "-p", str(pg_server.port), "-U", "postgres", name)
    return pg_server.url.rsplit("/", 1)[0] + f"/{name}"


@pytest.fixture
def pg_own(tmp_path: Path) -> Iterator[TempPostgres]:
    """A dedicated server this test may kill and restart."""
    server = TempPostgres(tmp_path).init()
    yield server
    server.destroy()
