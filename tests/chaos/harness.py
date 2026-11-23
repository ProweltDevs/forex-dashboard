"""Chaos harness: a real ``trader run`` process (``python -m trader.cli``) trading a cTrader account on
the fake cTrader server, which runs in the test process and so outlives the trader. The test can kill the
trader (SIGKILL or SIGTERM), drop or refuse its broker connection, and restart it on the same data
directory, then check that the broker and the trader agree.

Safety: the process gets a throwaway env file and an environment stripped of every TRADER_/CTRADER_/
EXNESS_/TELEGRAM_ variable, so it can never pick up real credentials; its only account is the fake.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

import httpx
import yaml

from tests.integration.ctrader_fake import FakeCTraderServer
from tests.integration.test_app_run import FAST_RISK

ACCOUNT = "ct-chaos"
TOKEN = "chaos-token-" + "x" * 32
SECRET_PREFIXES = ("TRADER_", "CTRADER_", "EXNESS_", "TELEGRAM_", "CT_CHAOS_")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def chaos_config(data_dir: Path, broker_port: int, api_port: int, ops_port: int) -> dict[str, Any]:
    return {
        "mode": "demo",
        "data_dir": str(data_dir),
        "logging": {"level": "INFO", "json": True},
        "market_data": {"timeframes": ["S1"], "warmup_bars": 0, "recorder": {"enabled": False}},
        "api": {"enabled": True, "port": api_port},
        "ops": {"enabled": True, "port": ops_port, "sample_s": 0.5},
        "accounts": [
            {
                "id": ACCOUNT,
                "broker": "ctrader",
                "environment": "demo",
                "account_id": "1001",
                "secrets_ref": "CT_CHAOS",
                "symbols": ["EURUSD"],
                "ctrader": {"host": "127.0.0.1", "port": broker_port, "use_tls": False},
                "risk": FAST_RISK,
            }
        ],
    }


@dataclass
class Trader:
    workdir: Path
    broker: FakeCTraderServer
    api_port: int = field(default_factory=free_port)
    ops_port: int = field(default_factory=free_port)
    proc: asyncio.subprocess.Process | None = None
    starts: int = 0
    _log: IO[bytes] | None = None

    @property
    def data_dir(self) -> Path:
        return self.workdir / "data"

    @property
    def log_path(self) -> Path:
        return self.workdir / "trader.log"

    def write_files(self) -> tuple[Path, Path]:
        config = self.workdir / "config.yaml"
        config.write_text(
            yaml.safe_dump(chaos_config(self.data_dir, self.broker.port, self.api_port, self.ops_port)),
            encoding="utf-8",
        )
        env_file = self.workdir / "chaos.env"
        env_file.write_text(
            "\n".join(
                [
                    f"CT_CHAOS_CLIENT_ID={self.broker.client_id}",
                    f"CT_CHAOS_CLIENT_SECRET={self.broker.client_secret}",
                    f"CT_CHAOS_ACCESS_TOKEN={self.broker.access_token}",
                    f"CT_CHAOS_REFRESH_TOKEN={self.broker.refresh_token}",
                    f"TRADER_API_TOKEN={TOKEN}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        env_file.chmod(0o600)
        return config, env_file

    async def start(self) -> None:
        config, env_file = self.write_files()
        env = {k: v for k, v in os.environ.items() if not k.startswith(SECRET_PREFIXES)}
        self._close_log()  # the previous run may have exited on its own
        self._log = self.log_path.open("ab")
        self._log.write(f"\n===== start {self.starts + 1} =====\n".encode())
        self._log.flush()
        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "trader.cli", "--env-file", str(env_file), "run", "--config", str(config),
            cwd=self.workdir, env=env, stdout=self._log, stderr=asyncio.subprocess.STDOUT,
        )  # fmt: skip
        self.starts += 1

    async def cli(self, *args: str) -> tuple[int, str]:
        """Run another ``trader`` command against the same config and data directory."""
        config, env_file = self.write_files()
        env = {k: v for k, v in os.environ.items() if not k.startswith(SECRET_PREFIXES)}
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "trader.cli", "--env-file", str(env_file), args[0], "--config", str(config),
            *args[1:], cwd=self.workdir, env=env, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )  # fmt: skip
        out, _ = await proc.communicate()
        return int(proc.returncode or 0), out.decode(errors="replace")

    def log(self) -> str:
        return self.log_path.read_text(encoding="utf-8", errors="replace")

    def events(self, name: str) -> list[dict[str, Any]]:
        out = []
        for line in self.log().splitlines():
            if line.startswith("{") and f'"event": "{name}"' in line:
                out.append(json.loads(line))
        return out

    async def health(self) -> tuple[int, dict[str, Any]]:
        async with httpx.AsyncClient(timeout=3) as c:
            resp = await c.get(f"http://127.0.0.1:{self.ops_port}/health")
            return resp.status_code, resp.json()

    async def metrics(self) -> str:
        async with httpx.AsyncClient(timeout=3) as c:
            return (await c.get(f"http://127.0.0.1:{self.ops_port}/metrics")).text

    async def wait_health(self, want: int = 200, timeout: float = 40.0) -> dict[str, Any]:
        body: dict[str, Any] = {}
        async with asyncio.timeout(timeout):
            while True:
                if self.proc is not None and self.proc.returncode is not None:
                    raise AssertionError(f"trader exited ({self.proc.returncode}):\n{self.log()[-3000:]}")
                try:
                    code, body = await self.health()
                    if code == want:
                        return body
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)

    def api(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{self.api_port}",
            headers={"Authorization": f"Bearer {TOKEN}"},
            timeout=30,
        )

    async def kill(self) -> None:
        """SIGKILL: no shutdown code runs at all, as in a power cut or an OOM kill."""
        assert self.proc is not None
        self.proc.send_signal(signal.SIGKILL)
        await self.proc.wait()
        self._close_log()

    async def terminate(self, timeout: float = 60.0) -> int:
        """SIGTERM (docker stop, systemd): the graceful shutdown path."""
        assert self.proc is not None
        self.proc.send_signal(signal.SIGTERM)
        async with asyncio.timeout(timeout):
            code = await self.proc.wait()
        self._close_log()
        return code

    def _close_log(self) -> None:
        if self._log is not None:
            self._log.close()
            self._log = None

    async def cleanup(self) -> None:
        if self.proc is not None and self.proc.returncode is None:
            await self.kill()
        self._close_log()

    def oms_orders(self) -> dict[str, str]:
        """client_order_id -> status, from the OMS snapshot on disk."""
        path = self.data_dir / "oms" / f"{ACCOUNT}.json"
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        return {o["request"]["client_order_id"]: o["status"] for o in data.get("orders", [])}


def order_body(side: str = "BUY", volume: str = "0.01") -> dict[str, str]:
    """A manual market order with a stop far from the fake's 1.10000/1.10010 quote."""
    sl = "1.09000" if side == "BUY" else "1.11000"
    return {"account": ACCOUNT, "symbol": "EURUSD", "side": side, "volume": volume, "sl": sl}


def executed(broker: FakeCTraderServer) -> dict[str, int]:
    """clientOrderId -> how many times the broker executed it (any number but 1 is a bug)."""
    counts: dict[str, int] = {}
    for order in broker.history:
        if order.clientOrderId:
            counts[order.clientOrderId] = counts.get(order.clientOrderId, 0) + 1
    return counts
