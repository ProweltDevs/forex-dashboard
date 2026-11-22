"""A fake Telegram Bot API server for tests: ``sendMessage`` records messages, ``getUpdates`` long-polls a
queue of updates the test pushes (offset acknowledges, like the real API). Wrong token: 401."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Generator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

BOT_TOKEN = "123456789:AAFakeTokenForTestsOnly_abcdefghijklm"
CHAT_ID = "4242"


class _Server(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None]:
        yield


@dataclass
class FakeTelegram:
    url: str = ""
    sent: list[str] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    next_id: int = 1
    arrived: asyncio.Event = field(default_factory=asyncio.Event)
    polls: int = 0

    def command(
        self, text: str, *, chat_id: str = CHAT_ID, chat_type: str = "private", age_s: int = 0
    ) -> None:
        self.updates.append(
            {
                "update_id": self.next_id,
                "message": {
                    "message_id": self.next_id,
                    "date": int(time.time()) - age_s,
                    "chat": {"id": int(chat_id), "type": chat_type},
                    "from": {"id": int(chat_id), "is_bot": False, "first_name": "t"},
                    "text": text,
                },
            }
        )
        self.next_id += 1
        self.arrived.set()

    def texts(self, needle: str) -> list[str]:
        return [t for t in self.sent if needle in t]

    async def wait_text(self, needle: str, timeout: float = 15.0) -> str:
        async with asyncio.timeout(timeout):
            while not self.texts(needle):
                await asyncio.sleep(0.05)
        return self.texts(needle)[0]


def _app(fake: FakeTelegram) -> FastAPI:
    api = FastAPI()

    @api.post("/bot{token}/{method}")
    async def call(token: str, method: str, request: Request) -> JSONResponse:
        if token != BOT_TOKEN:
            return JSONResponse({"ok": False, "error_code": 401, "description": "Unauthorized"}, 401)
        body: dict[str, Any] = await request.json()
        if method == "sendMessage":
            if str(body.get("chat_id")) != CHAT_ID or not body.get("text"):
                return JSONResponse({"ok": False, "error_code": 400, "description": "Bad Request"}, 400)
            fake.sent.append(str(body["text"]))
            return JSONResponse({"ok": True, "result": {"message_id": len(fake.sent)}})
        if method == "getUpdates":
            fake.polls += 1
            offset = body.get("offset")
            if isinstance(offset, int):
                fake.updates = [u for u in fake.updates if u["update_id"] >= offset]
            if not fake.updates:
                fake.arrived.clear()
                with contextlib.suppress(TimeoutError):
                    async with asyncio.timeout(min(float(body.get("timeout", 0)), 0.5)):
                        await fake.arrived.wait()
            return JSONResponse({"ok": True, "result": list(fake.updates)})
        return JSONResponse({"ok": False, "error_code": 404, "description": "Not Found"}, 404)

    return api


@asynccontextmanager
async def fake_telegram() -> AsyncIterator[FakeTelegram]:
    fake = FakeTelegram()
    server = _Server(
        uvicorn.Config(
            _app(fake),
            host="127.0.0.1",
            port=0,
            http="h11",
            lifespan="off",
            log_config=None,
            access_log=False,
        )
    )
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    fake.url = f"http://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
    try:
        yield fake
    finally:
        server.should_exit = True
        await task
