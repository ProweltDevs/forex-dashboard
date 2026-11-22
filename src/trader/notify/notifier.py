"""Notifier: dedupe, rate limiting and background delivery of notices to channels (SPEC §14).

- ``notify(notice)`` is synchronous and never blocks or raises: trading never waits on Telegram or SMTP.
- **Dedupe:** a notice whose ``key`` was sent less than ``dedupe`` ago is dropped and counted; the next
  one with that key after the window says how many were folded in.
- **Rate limit:** at most ``rate_per_min`` messages per channel per minute. Extra non-critical notices are
  counted and a summary goes out once there is room. Critical notices (kill switch, database down) are
  never rate-limited.
- **Delivery:** one queue and worker per channel; up to 3 attempts per message, honouring a channel's
  retry-after; failures are logged without the exception text (it can hold a URL with a token).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
from typing import Protocol

from trader.domain.time import Clock
from trader.observability.logging import get_logger

_log = get_logger("trader.notify")


class Severity(IntEnum):
    INFO = 10
    WARNING = 20
    CRITICAL = 30

    @classmethod
    def parse(cls, value: str) -> Severity:
        return cls[value.upper()]

    @property
    def tag(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True, kw_only=True)
class Notice:
    key: str  # dedupe key, e.g. "reject:paper-main:ema:loss_per_trade"
    severity: Severity
    title: str
    text: str
    ts: datetime
    account_id: str | None = None


class ChannelError(Exception):
    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class Channel(Protocol):
    name: str
    min_severity: Severity

    async def send(self, subject: str, text: str) -> None: ...

    async def close(self) -> None: ...


@dataclass(slots=True)
class NotifierStats:
    accepted: int = 0
    deduped: int = 0
    filtered: int = 0  # below a channel's min severity
    rate_limited: int = 0
    sent: int = 0
    failed: int = 0
    dropped: int = 0  # queue full


@dataclass(slots=True)
class _Lane:
    channel: Channel
    queue: deque[tuple[Notice, str, str]] = field(default_factory=deque)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    sent_at: deque[datetime] = field(default_factory=deque)
    suppressed: int = 0
    task: asyncio.Task[None] | None = None


QUEUE_LIMIT = 500
ATTEMPTS = 3


class Notifier:
    def __init__(
        self,
        channels: list[Channel],
        clock: Clock,
        *,
        dedupe: timedelta = timedelta(minutes=5),
        rate_per_min: int = 20,
        prefix: str = "",
        retry_delay: timedelta = timedelta(seconds=2),
    ) -> None:
        self._lanes = [_Lane(ch) for ch in channels]
        self._clock = clock
        self._dedupe = dedupe
        self._rate = rate_per_min
        self._prefix = prefix
        self._retry_delay = retry_delay
        self._seen: dict[str, tuple[datetime, int]] = {}  # key -> (last sent, folded since)
        self.stats = NotifierStats()
        self.history: deque[Notice] = deque(maxlen=200)  # what was accepted, newest last (tests, /status)

    @property
    def channels(self) -> list[Channel]:
        return [lane.channel for lane in self._lanes]

    # ------------------------------------------------------------------ API

    def notify(self, notice: Notice) -> None:
        try:
            self._accept(notice)
        except Exception:
            _log.exception("notify_failed", key=notice.key)

    def _accept(self, notice: Notice) -> None:
        now = self._clock.now()
        last = self._seen.get(notice.key)
        if last is not None and now - last[0] < self._dedupe:
            self._seen[notice.key] = (last[0], last[1] + 1)
            self.stats.deduped += 1
            return
        folded = last[1] if last is not None else 0
        self._seen[notice.key] = (now, 0)
        if len(self._seen) > 10_000:  # forget keys older than the window
            self._seen = {k: v for k, v in self._seen.items() if now - v[0] < self._dedupe}
        self.stats.accepted += 1
        self.history.append(notice)
        subject = f"{self._prefix}{notice.severity.tag}: {notice.title}"
        text = f"{subject}\n{notice.text}"
        if folded:
            text += f"\n(+{folded} similar in the last {int(self._dedupe.total_seconds() // 60)} min)"
        for lane in self._lanes:
            if notice.severity < lane.channel.min_severity:
                self.stats.filtered += 1
                continue
            if len(lane.queue) >= QUEUE_LIMIT:
                lane.queue.popleft()
                self.stats.dropped += 1
            lane.queue.append((notice, subject, text))
            lane.wake.set()

    async def start(self) -> None:
        for lane in self._lanes:
            lane.task = asyncio.create_task(self._worker(lane), name=f"notify-{lane.channel.name}")

    async def stop(self, timeout: timedelta = timedelta(seconds=10)) -> None:
        """Deliver what is queued (bounded by ``timeout``), then close the channels."""
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(timeout.total_seconds()):
                while any(lane.queue for lane in self._lanes):
                    await asyncio.sleep(0.05)
        for lane in self._lanes:
            if lane.task is not None:
                lane.task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await lane.task
            await lane.channel.close()

    # ------------------------------------------------------------------ delivery

    def _room(self, lane: _Lane, now: datetime) -> bool:
        while lane.sent_at and now - lane.sent_at[0] >= timedelta(minutes=1):
            lane.sent_at.popleft()
        return len(lane.sent_at) < self._rate

    async def _worker(self, lane: _Lane) -> None:
        while True:
            if not lane.queue:
                lane.wake.clear()
                with contextlib.suppress(TimeoutError):
                    async with asyncio.timeout(1.0 if lane.suppressed else None):
                        await lane.wake.wait()
            now = self._clock.now()
            if lane.suppressed and self._room(lane, now) and not lane.queue:
                count, lane.suppressed = lane.suppressed, 0
                await self._deliver(
                    lane,
                    f"{self._prefix}INFO: notifications suppressed",
                    f"{count} notifications were not sent (rate limit {self._rate}/min); see the logs.",
                )
                continue
            if not lane.queue:
                continue
            notice, subject, text = lane.queue.popleft()
            if notice.severity < Severity.CRITICAL and not self._room(lane, now):
                lane.suppressed += 1
                self.stats.rate_limited += 1
                continue
            await self._deliver(lane, subject, text)

    async def _deliver(self, lane: _Lane, subject: str, text: str) -> None:
        lane.sent_at.append(self._clock.now())
        for attempt in range(1, ATTEMPTS + 1):
            try:
                await lane.channel.send(subject, text)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                retry_after = exc.retry_after if isinstance(exc, ChannelError) else None
                _log.warning(
                    "notify_send_failed",
                    channel=lane.channel.name,
                    attempt=attempt,
                    error=type(exc).__name__,
                    detail=str(exc) if isinstance(exc, ChannelError) else None,  # ours: never holds a token
                )
                if attempt == ATTEMPTS:
                    self.stats.failed += 1
                    return
                delay = (
                    retry_after if retry_after is not None else self._retry_delay.total_seconds() * attempt
                )
                await asyncio.sleep(min(delay, 60.0))
            else:
                self.stats.sent += 1
                return
