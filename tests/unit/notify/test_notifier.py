from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from trader.domain.time import FixedClock
from trader.notify.notifier import ChannelError, Notice, Notifier, Severity

T0 = datetime(2026, 3, 2, 9, 0, tzinfo=UTC)


class FakeChannel:
    def __init__(self, name: str = "fake", min_severity: Severity = Severity.INFO, fail: int = 0) -> None:
        self.name = name
        self.min_severity = min_severity
        self.fail = fail
        self.sent: list[tuple[str, str]] = []
        self.closed = False

    async def send(self, subject: str, text: str) -> None:
        if self.fail:
            self.fail -= 1
            raise ChannelError("boom", retry_after=0)
        self.sent.append((subject, text))

    async def close(self) -> None:
        self.closed = True


def notice(key: str, sev: Severity = Severity.WARNING, title: str = "t") -> Notice:
    return Notice(key=key, severity=sev, title=title, text="body", ts=T0)


async def settle(cond: object = None, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while True:
            await asyncio.sleep(0.01)
            if cond is None or cond():  # type: ignore[operator]
                return


async def test_dedupe_folds_repeats_and_says_so_next_time() -> None:
    clock, ch = FixedClock(T0), FakeChannel()
    n = Notifier([ch], clock, dedupe=timedelta(minutes=5), prefix="[paper] ")
    await n.start()
    n.notify(notice("k"))
    n.notify(notice("k"))
    n.notify(notice("k"))
    await settle(lambda: ch.sent)
    clock.advance(timedelta(minutes=6))
    n.notify(notice("k"))
    await settle(lambda: len(ch.sent) == 2)
    await n.stop()
    assert ch.sent[0] == ("[paper] WARNING: t", "[paper] WARNING: t\nbody")
    assert ch.sent[1][1].endswith("(+2 similar in the last 5 min)")
    assert n.stats.deduped == 2
    assert ch.closed


async def test_rate_limit_counts_extras_but_never_blocks_critical() -> None:
    clock, ch = FixedClock(T0), FakeChannel()
    n = Notifier([ch], clock, rate_per_min=3)
    await n.start()
    for i in range(5):
        n.notify(notice(f"w{i}"))
    n.notify(notice("crit", Severity.CRITICAL, "Kill switch FLATTEN_ALL"))
    await settle(lambda: len(ch.sent) == 4)
    assert [s for s, _ in ch.sent] == ["WARNING: t"] * 3 + ["CRITICAL: Kill switch FLATTEN_ALL"]
    assert n.stats.rate_limited == 2
    clock.advance(timedelta(seconds=61))  # room again: the summary goes out
    await settle(lambda: len(ch.sent) == 5)
    assert ch.sent[4][1].endswith("2 notifications were not sent (rate limit 3/min); see the logs.")
    await n.stop()


async def test_channels_have_their_own_minimum_severity() -> None:
    chat, mail = FakeChannel("chat"), FakeChannel("mail", Severity.WARNING)
    n = Notifier([chat, mail], FixedClock(T0))
    await n.start()
    n.notify(notice("i", Severity.INFO))
    n.notify(notice("w", Severity.WARNING))
    await n.stop()
    assert len(chat.sent) == 2
    assert [s for s, _ in mail.sent] == ["WARNING: t"]
    assert n.stats.filtered == 1


async def test_failed_sends_are_retried_then_given_up() -> None:
    flaky, dead = FakeChannel("flaky", fail=2), FakeChannel("dead", fail=99)
    n = Notifier([flaky, dead], FixedClock(T0), retry_delay=timedelta(0))
    await n.start()
    n.notify(notice("k"))
    await settle(lambda: flaky.sent and n.stats.failed == 1)
    await n.stop()
    assert len(flaky.sent) == 1
    assert dead.sent == []
    assert (n.stats.sent, n.stats.failed) == (1, 1)


async def test_notify_never_raises_and_stop_delivers_the_backlog() -> None:
    ch = FakeChannel()
    n = Notifier([ch], FixedClock(T0))
    await n.start()
    for i in range(10):
        n.notify(notice(f"k{i}", Severity.INFO))
    await n.stop()  # drains first
    assert len(ch.sent) == 10
