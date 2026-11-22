from __future__ import annotations

import json
import smtplib
from typing import Any, ClassVar

import httpx
import pytest

from trader.notify.mail import EmailChannel
from trader.notify.notifier import ChannelError
from trader.notify.telegram import MAX_TEXT, TelegramChannel
from trader.observability.logging import redact

TOKEN = "123456789:AAH-secret-token-value-0123456789abc"


def channel(handler: Any) -> TelegramChannel:
    return TelegramChannel(TOKEN, "42", client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))


async def test_send_message_payload() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

    ch = channel(handler)
    await ch.send("subject", "x" * 5000)
    await ch.close()
    [req] = seen
    assert req.url == f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    body = json.loads(req.content)
    assert body == {"chat_id": "42", "text": "x" * MAX_TEXT}


async def test_rate_limit_and_errors_never_carry_the_token() -> None:
    def limited(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 7}})

    with pytest.raises(ChannelError) as info:
        await channel(limited).send("", "t")
    assert info.value.retry_after == 7.0

    def refused(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"ok": False, "description": "Bad Request: chat not found"})

    with pytest.raises(ChannelError, match="chat not found"):
        await channel(refused).send("", "t")

    def broken(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"cannot reach {req.url}")  # httpx puts the URL in the text

    with pytest.raises(ChannelError) as err:
        await channel(broken).send("", "t")
    assert TOKEN not in str(err.value)
    assert str(err.value) == "telegram sendMessage: ConnectError"


async def test_get_updates() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        assert json.loads(req.content) == {"timeout": 5, "allowed_updates": ["message"], "offset": 11}
        return httpx.Response(200, json={"ok": True, "result": [{"update_id": 11}, "junk"]})

    assert await channel(handler).get_updates(11, 5) == [{"update_id": 11}]


def test_bot_tokens_are_redacted_from_logs() -> None:
    line = f'HTTP Request: POST https://api.telegram.org/bot{TOKEN}/sendMessage "HTTP/1.1 200 OK"'
    out = redact(line)
    assert isinstance(out, str)
    assert TOKEN not in out
    assert "/bot***REDACTED***/sendMessage" in out


class FakeSMTP:
    instances: ClassVar[list[FakeSMTP]] = []
    fail: ClassVar[bool] = False

    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.calls: list[tuple[str, Any]] = [("connect", (host, port))]
        FakeSMTP.instances.append(self)

    def __enter__(self) -> FakeSMTP:
        return self

    def __exit__(self, *exc: object) -> None:
        self.calls.append(("quit", None))

    def starttls(self, context: object) -> None:
        self.calls.append(("starttls", None))

    def login(self, user: str, password: str) -> None:
        if FakeSMTP.fail:
            raise smtplib.SMTPAuthenticationError(535, f"bad password {password}".encode())
        self.calls.append(("login", user))

    def send_message(self, msg: Any) -> None:
        self.calls.append(("send", (msg["Subject"], msg["To"], msg.get_content())))


async def test_email(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(smtplib, "SMTP", FakeSMTP)
    ch = EmailChannel(
        host="smtp.example.test",
        port=587,
        sender="trader@example.test",
        recipients=("me@example.test", "ops@example.test"),
        user="u",
        password="hunter2",
    )
    await ch.send("[live] CRITICAL: Kill switch", "body\n")
    [smtp] = FakeSMTP.instances
    assert smtp.calls == [
        ("connect", ("smtp.example.test", 587)),
        ("starttls", None),
        ("login", "u"),
        ("send", ("[live] CRITICAL: Kill switch", "me@example.test, ops@example.test", "body\n")),
        ("quit", None),
    ]
    FakeSMTP.fail = True
    with pytest.raises(ChannelError) as info:
        await ch.send("s", "t")
    assert "hunter2" not in str(info.value)
