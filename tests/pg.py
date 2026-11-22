"""A throwaway PostgreSQL server for tests: ``initdb`` into a temp directory, start it on a free port,
kill it hard (the DB outage) and start it again. Uses the PostgreSQL binaries on PATH or under
/usr/lib/postgresql/*/bin; tests skip when there are none. As root it runs the server as ``postgres``
(PostgreSQL refuses to run as root)."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path

import pytest


def _bin_dir() -> Path | None:
    found = shutil.which("initdb")
    if found:
        return Path(found).parent
    candidates = sorted(Path("/usr/lib/postgresql").glob("*/bin/initdb"), reverse=True)
    return candidates[0].parent if candidates else None


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class TempPostgres:
    def __init__(self, base: Path) -> None:
        bin_dir = _bin_dir()
        if bin_dir is None:
            pytest.skip("PostgreSQL binaries not found (initdb)")
        self.bin = bin_dir
        self._as_postgres = os.geteuid() == 0
        if self._as_postgres and shutil.which("runuser") is None:
            pytest.skip("running as root without runuser")
        if self._as_postgres:
            # pytest's tmp dirs are 0700 root: the postgres user could not reach them.
            self.root = Path(tempfile.mkdtemp(prefix="trader-pg-"))
            shutil.chown(self.root, "postgres", "postgres")
        else:
            self.root = base / "pg"
            self.root.mkdir(parents=True, exist_ok=True)
        self.data = self.root / "data"
        self.port = _free_port()

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        cmd = [str(self.bin / args[0]), *args[1:]]
        if self._as_postgres:
            cmd = ["runuser", "-u", "postgres", "--", *cmd]
        return subprocess.run(cmd, check=check, capture_output=True, text=True, timeout=60)

    @property
    def url(self) -> str:
        return f"postgresql+asyncpg://postgres@127.0.0.1:{self.port}/trader"

    def init(self) -> TempPostgres:
        self._run("initdb", "-D", str(self.data), "-U", "postgres", "-A", "trust", "--no-sync", "-E", "UTF8")
        self.start()
        self._run("createdb", "-h", "127.0.0.1", "-p", str(self.port), "-U", "postgres", "trader")
        return self

    def start(self) -> None:
        opts = f"-p {self.port} -k {self.root} -c listen_addresses=127.0.0.1 -c fsync=off"
        self._run("pg_ctl", "-D", str(self.data), "-o", opts, "-l", str(self.root / "log"), "-w", "start")

    def kill(self) -> None:
        """Hard stop (no checkpoint, connections dropped): the DB outage of the acceptance test."""
        self._run("pg_ctl", "-D", str(self.data), "-m", "immediate", "-w", "stop", check=False)

    def stop(self) -> None:
        self._run("pg_ctl", "-D", str(self.data), "-m", "fast", "-w", "stop", check=False)

    def destroy(self) -> None:
        self.stop()
        shutil.rmtree(self.root, ignore_errors=True)
