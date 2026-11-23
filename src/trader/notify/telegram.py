"""Telegram Bot API channel and command bot (SPEC §14). See ``docs/notifications.md`` for the Bot API facts
this relies on and how they were checked.

- ``TelegramChannel.send``: ``POST /bot<token>/sendMessage`` with ``chat_id`` and plain ``text`` (no
  ``parse_mode``: nothing to escape). Over 4096 characters is cut. HTTP 429 carries
  ``parameters.retry_after`` (seconds), honoured by the Notifier.
- ``TelegramBot``: long polling with ``getUpdates`` (``offset`` = last ``update_id`` + 1, ``timeout``
  seconds). Commands are accepted only from the configured chat, and only if it is a private chat (in a
  group every member could send them). Messages older than 2 minutes are ignored: a ``/kill`` typed while
  the process was down is not replayed hours later. Every command is audited (actor ``telegram``).

The bot token is part of the URL path. It is never logged: errors are reported by type and HTTP status, and
the log redaction also scrubs anything shaped like a bot token.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import httpx

from trader.domain.enums import KillLevel
from trader.domain.time import Clock
from trader.observability.logging import get_logger

from .notifier import ChannelError, Severity

if TYPE_CHECKING:
    from trader.api.control import Control

_log = get_logger("trader.notify.telegram")

API = "https://api.telegram.org"
MAX_TEXT = 4096
MAX_AGE = timedelta(minutes=2)


def _check(resp: httpx.Response) -> object:
    try:
        body = resp.json()
    except ValueError:
        body = None
    data = body if isinstance(body, dict) else {}
    if resp.status_code == 429:
        params = data.get("parameters")
        retry = params.get("retry_after") if isinstance(params, dict) else None
        raise ChannelError(
            "telegram rate limit (HTTP 429)", retry_after=float(retry) if isinstance(retry, int) else 5.0
        )
    if resp.status_code != 200 or data.get("ok") is not True:
        description = data.get("description")
        raise ChannelError(
            f"telegram HTTP {resp.status_code}: {description if isinstance(description, str) else '-'}"
        )
    return data.get("result")


class TelegramChannel:
    name = "telegram"

    def __init__(
        self,
        token: str,
        chat_id: str,
        *,
        min_severity: Severity = Severity.INFO,
        api_base: str = API,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base = f"{api_base.rstrip('/')}/bot{token}"
        self.chat_id = chat_id
        self.min_severity = min_severity
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(15.0, read=40.0))

    async def send(self, subject: str, text: str) -> None:
        del subject  # the text starts with it
        await self._call("sendMessage", {"chat_id": self.chat_id, "text": text[:MAX_TEXT]})

    async def get_updates(self, offset: int | None, timeout_s: int) -> list[dict[str, object]]:
        params: dict[str, object] = {"timeout": timeout_s, "allowed_updates": ["message"]}
        if offset is not None:
            params["offset"] = offset
        result = await self._call("getUpdates", params)
        return [u for u in result if isinstance(u, dict)] if isinstance(result, list) else []

    async def _call(self, method: str, payload: dict[str, object]) -> object:
        try:
            resp = await self._client.post(f"{self._base}/{method}", json=payload)
        except httpx.HTTPError as exc:  # its text may hold the URL, and the URL holds the token
            raise ChannelError(f"telegram {method}: {type(exc).__name__}") from None
        return _check(resp)

    async def close(self) -> None:
        await self._client.aclose()


type Handler = Callable[[list[str]], Awaitable[str]]

HELP = (
    "/status - accounts, kill switch, strategies\n"
    "/pnl - today's P&L per account\n"
    "/pause [strategy|all] - pause strategies (default all)\n"
    "/kill [pause|cancel|flatten] [account|all] - kill switch (default: pause new entries, all accounts)\n"
    "Reset the kill switch with the API (POST /risk/reset),\n"
    "or `trader kill --reset` once the process is stopped."
)
_LEVELS = {
    "pause": KillLevel.PAUSE_NEW,
    "cancel": KillLevel.CANCEL_PENDING,
    "flatten": KillLevel.FLATTEN_ALL,
}


class TelegramBot:
    def __init__(self, channel: TelegramChannel, control: Control, clock: Clock, *, poll_s: int = 25) -> None:
        self._channel = channel
        self._control = control
        self._clock = clock
        self._poll_s = poll_s
        self._offset: int | None = None
        self._task: asyncio.Task[None] | None = None
        self._handlers: dict[str, Handler] = {
            "/status": self._status,
            "/pnl": self._pnl,
            "/pause": self._pause,
            "/kill": self._kill,
            "/help": self._help,
            "/start": self._help,
        }

    async def start(self) -> None:
        self._task = asyncio.create_task(self._poll(), name="telegram-bot")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _poll(self) -> None:
        backoff = 1.0
        while True:
            try:
                updates = await self._channel.get_updates(self._offset, self._poll_s)
                backoff = 1.0
            except ChannelError as exc:
                _log.warning("telegram_poll_failed", detail=str(exc))
                await asyncio.sleep(exc.retry_after or backoff)
                backoff = min(backoff * 2, 60.0)
                continue
            except Exception as exc:  # never let the bot die; never log the text (it may hold the URL)
                _log.warning("telegram_poll_failed", error=type(exc).__name__)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue
            for update in updates:
                uid = update.get("update_id")
                if isinstance(uid, int):
                    self._offset = uid + 1
                try:
                    await self.handle(update)
                except Exception:
                    _log.exception("telegram_command_failed")

    async def handle(self, update: dict[str, object]) -> str | None:
        """Run one update's command; returns the reply sent (None when ignored)."""
        msg = update.get("message")
        if not isinstance(msg, dict):
            return None
        chat = msg.get("chat")
        text = msg.get("text")
        if not isinstance(chat, dict) or not isinstance(text, str) or not text.startswith("/"):
            return None
        if str(chat.get("id")) != self._channel.chat_id or chat.get("type") != "private":
            _log.warning("telegram_command_refused", chat_type=chat.get("type"))  # never log who or what
            return None
        sent = msg.get("date")
        if isinstance(sent, int) and self._clock.now() - datetime.fromtimestamp(sent, UTC) > MAX_AGE:
            _log.info("telegram_command_stale", age_s=int(self._clock.now().timestamp()) - sent)
            return None
        parts = text.split()
        command = parts[0].split("@")[0].lower()  # "/status@MyBot" in some clients
        handler = self._handlers.get(command)
        if handler is None:
            reply = f"unknown command {command}\n{HELP}"
        else:
            try:
                reply = await handler(parts[1:])
            except Exception as exc:
                reply = f"{command} failed: {type(exc).__name__}: {exc}"
        try:
            await self._channel.send("", reply)
        except ChannelError as exc:
            _log.warning("telegram_reply_failed", detail=str(exc))
        return reply

    # ------------------------------------------------------------------ commands

    async def _help(self, _args: list[str]) -> str:
        return HELP

    async def _status(self, _args: list[str]) -> str:
        c = self._control
        lines = []
        for acc in c.accounts():
            st = c.account_state(acc.id)
            link = "connected" if acc.connected else "DISCONNECTED"
            lines.append(
                f"{acc.id} ({acc.broker} {acc.environment}): {link}, "
                f"kill {acc.kill_level}, equity {st.equity or '-'}, positions {st.positions}, "
                f"orders {st.open_orders}"
            )
        by_status: dict[str, list[str]] = {}
        for s in c.strategies():
            by_status.setdefault(s.status, []).append(s.id)
        lines.append(
            "strategies: "
            + ("; ".join(f"{k} {', '.join(v)}" for k, v in sorted(by_status.items())) or "none")
        )
        return "\n".join(lines)

    async def _pnl(self, _args: list[str]) -> str:
        c = self._control
        lines = []
        for acc in c.accounts():
            p = c.pnl(acc.id)
            lines.append(
                f"{acc.id} {p.day}: realized {p.net:.2f} ({p.deals} deals), unrealized "
                f"{'-' if p.unrealized is None else f'{p.unrealized:.2f}'}, equity "
                f"{'-' if p.equity is None else f'{p.equity:.2f}'}, drawdown "
                f"{'-' if p.drawdown is None else f'{p.drawdown:.2f}'}"
            )
        return "\n".join(lines) or "no accounts"

    async def _pause(self, args: list[str]) -> str:
        c = self._control
        target = args[0] if args else "all"
        running = [s.id for s in c.strategies() if s.status == "RUNNING"]
        ids = running if target == "all" else [target]
        done = []
        for sid in ids:
            await c.pause_strategy(sid, "paused via Telegram")
            done.append(sid)
        return f"paused: {', '.join(done)}" if done else "nothing running"

    async def _kill(self, args: list[str]) -> str:
        c = self._control
        level = _LEVELS.get(args[0].lower() if args else "pause")
        if level is None:
            return f"unknown level {args[0]!r}; use pause, cancel or flatten"
        target = args[1] if len(args) > 1 else "all"
        reason = "kill via Telegram"
        if target == "all":
            out = await c.kill_all(level, reason)
        else:
            out = [await c.kill(target, level, reason)]
        return "\n".join(f"{r.account_id}: kill switch {r.level}" for r in out)
