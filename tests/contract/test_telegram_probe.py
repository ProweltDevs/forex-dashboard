"""Probe: the Telegram Bot API facts ``trader.notify.telegram`` relies on (docs/notifications.md).

Network test, ``-m demo``. Needs ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_CHAT_ID`` (a private chat with your
bot: send it any message first) in ``.env`` or the environment; skipped without them. It sends three short
messages to that chat. It never places an order and touches no trading account.

What it checks:
1. ``sendMessage`` with ``chat_id`` + plain ``text`` returns ``{"ok": true, "result": {"message_id": ...}}``.
2. A 4096-character text is accepted (our cut-off), and 4097 is refused with ``ok: false`` + ``description``.
3. An unknown chat is refused with HTTP 400, ``ok: false`` and a ``description``.
4. ``getUpdates`` with ``timeout``/``allowed_updates`` returns a list of updates with ``update_id``, and a
   private chat's message has ``chat.type == "private"`` and an int ``date`` (unix seconds).
"""

from __future__ import annotations

import httpx
import pytest

from tests.contract.harness import demo_env
from trader.notify.notifier import ChannelError
from trader.notify.telegram import API, MAX_TEXT, TelegramChannel

pytestmark = pytest.mark.demo


@pytest.fixture
def creds() -> tuple[str, str]:
    env = demo_env()
    token, chat = env.get("TELEGRAM_BOT_TOKEN", ""), env.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat:
        pytest.skip("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set")
    return token, chat


async def test_bot_api_facts(creds: tuple[str, str]) -> None:
    token, chat = creds
    ch = TelegramChannel(token, chat)
    try:
        await ch.send("", "trader probe 1/3: sendMessage, plain text")
        await ch.send("", "trader probe 2/3: " + "x" * (MAX_TEXT - 18))  # exactly 4096 characters
        async with httpx.AsyncClient(timeout=15) as raw:
            resp = await raw.post(
                f"{API}/bot{token}/sendMessage", json={"chat_id": chat, "text": "x" * (MAX_TEXT + 1)}
            )
            body = resp.json()
            assert body["ok"] is False
            assert isinstance(body["description"], str)
            print(f"[telegram-probe] 4097 chars: HTTP {resp.status_code} {body['description']}")
        bad = TelegramChannel(token, "1")  # not a chat this bot has
        try:
            with pytest.raises(ChannelError, match="telegram HTTP 400"):
                await bad.send("", "nobody")
        finally:
            await bad.close()
        updates = await ch.get_updates(None, 0)
        for u in updates:
            assert isinstance(u["update_id"], int)
        private = [
            u["message"]
            for u in updates
            if isinstance(u.get("message"), dict) and str(u["message"]["chat"]["id"]) == chat  # type: ignore[index]
        ]
        for m in private:
            assert m["chat"]["type"] == "private"  # type: ignore[index]
            assert isinstance(m["date"], int)  # type: ignore[index]
        print(f"[telegram-probe] getUpdates: {len(updates)} updates, {len(private)} from the chat")
        await ch.send("", "trader probe 3/3: done")
    finally:
        await ch.close()
