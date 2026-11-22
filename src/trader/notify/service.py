"""Notifications: the Notifier, the event rules and (optionally) the Telegram bot, wired to a running app.

``attach(bus)`` turns that bus's events into notices. The subscriptions are bounded (``DROP_OLDEST``):
notifications are best effort and must never hold memory or the event loop hostage.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Mapping
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

from trader.config.settings import AppConfig
from trader.domain.errors import ConfigError
from trader.domain.time import Clock
from trader.marketdata.bus import Bus, Overflow, Subscription
from trader.observability.logging import get_logger

from .mail import EmailChannel
from .notifier import Channel, Notice, Notifier, Severity
from .rules import EventRules, OrderLookup
from .telegram import TelegramBot, TelegramChannel

if TYPE_CHECKING:
    from trader.api.control import Control

_log = get_logger("trader.notify.service")


def _env(env: Mapping[str, str], name: str, what: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigError(f"notify.{what} is configured but {name} is not set")
    return value


class Notifications:
    def __init__(
        self, notifier: Notifier, rules: EventRules, clock: Clock, telegram: TelegramChannel | None = None
    ) -> None:
        self.notifier = notifier
        self.rules = rules
        self._clock = clock
        self._telegram = telegram
        self.bot: TelegramBot | None = None
        self._subs: list[Subscription[Any]] = []
        self._tasks: list[asyncio.Task[None]] = []

    @classmethod
    def from_config(
        cls,
        config: AppConfig,
        env: Mapping[str, str],
        clock: Clock,
        *,
        channels: list[Channel] | None = None,
    ) -> Notifications | None:
        """None when nothing is configured. ``channels`` replaces the configured ones (tests)."""
        n = config.notify
        telegram: TelegramChannel | None = None
        if channels is None:
            channels = []
            if n.telegram is not None:
                telegram = TelegramChannel(
                    _env(env, n.telegram.token_env, "telegram"),
                    _env(env, n.telegram.chat_id_env, "telegram"),
                    min_severity=Severity.parse(n.telegram.min_severity),
                    api_base=n.telegram.api_base,
                )
                channels.append(telegram)
            if n.email is not None:
                e = n.email
                channels.append(
                    EmailChannel(
                        host=e.host,
                        port=e.port,
                        sender=e.sender,
                        recipients=e.recipients,
                        user=env.get(e.user_env) or None,
                        password=env.get(e.password_env) or None,
                        starttls=e.starttls,
                        min_severity=Severity.parse(e.min_severity),
                    )
                )
        else:
            telegram = next((c for c in channels if isinstance(c, TelegramChannel)), None)
        if not channels:
            return None
        notifier = Notifier(
            channels,
            clock,
            dedupe=timedelta(seconds=n.dedupe_s),
            rate_per_min=n.rate_per_min,
            prefix=f"[{config.mode}] ",
        )
        return cls(notifier, EventRules(fills=n.fills), clock, telegram)

    def set_order_lookup(self, lookup: OrderLookup) -> None:
        self.rules.orders = lookup

    async def start(self) -> None:
        await self.notifier.start()

    def attach(self, bus: Bus[Any], name: str) -> None:
        sub = bus.subscribe(f"notify-{name}", policy=Overflow.DROP_OLDEST, maxsize=5_000)
        self._subs.append(sub)
        self._tasks.append(asyncio.create_task(self._consume(sub), name=f"notify-{name}"))

    async def _consume(self, sub: Subscription[Any]) -> None:
        async for event in sub:
            self._handle(event)

    def _handle(self, event: object) -> None:
        try:
            notice = self.rules.notice(event)
        except Exception:
            _log.exception("notify_rule_failed", kind=type(event).__name__)
            return
        if notice is not None:
            self.notifier.notify(notice)

    async def enable_commands(self, control: Control) -> None:
        if self._telegram is None:
            raise ConfigError("notify.telegram.commands needs notify.telegram")
        self.bot = TelegramBot(self._telegram, control, self._clock)
        await self.bot.start()

    def info(self, key: str, title: str, text: str, severity: Severity = Severity.INFO) -> None:
        self.notifier.notify(Notice(key=key, severity=severity, title=title, text=text, ts=self._clock.now()))

    def daily_report(self, account_id: str, day: date, lines: list[str]) -> None:
        self.info(
            f"daily:{account_id}:{day.isoformat()}", f"Daily report {account_id} {day}", "\n".join(lines)
        )

    async def stop(self) -> None:
        """Turn what the subscriptions already hold into notices, then deliver and close."""
        if self.bot is not None:
            await self.bot.stop()
        for sub in self._subs:
            while (event := sub.get_nowait()) is not None:
                self._handle(event)
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self.notifier.stop()
