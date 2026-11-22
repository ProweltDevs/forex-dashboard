"""Rate limiter, FX conversion, accounting maths, registry and broker-port value objects."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tests.factories import T0, eurusd
from trader.brokers.base import Broker, OrderChanges, RateLimit, RateLimits
from trader.brokers.paper import FakeTickSource, PaperBroker
from trader.brokers.paper.accounting import margin_required, pnl_quote, weighted_entry
from trader.brokers.ratelimit import TokenBucket
from trader.brokers.registry import BrokerDeps, BrokerRegistry, default_registry, paper_settings
from trader.config.settings import AccountConfig
from trader.domain import Side, Tick
from trader.domain.errors import ConfigError, ConversionUnavailable
from trader.domain.fx import conversion_rate
from trader.domain.time import FixedClock

D = Decimal
prices = st.decimals(min_value=D("0.5"), max_value=D(3000), places=5)
lots = st.decimals(min_value=D("0.01"), max_value=D(100), places=2)

# ------------------------------------------------------------------ token bucket


class FakeNs:
    def __init__(self) -> None:
        self.now = 0

    def __call__(self) -> int:
        return self.now


def test_token_bucket_burst_and_refill() -> None:
    clock = FakeNs()
    bucket = TokenBucket(RateLimit(name="x", rate_per_s=D(10), burst=5), now_ns=clock)
    assert [bucket.try_acquire() for _ in range(5)] == [0] * 5
    assert bucket.try_acquire() == 100  # 1 token at 10/s
    clock.now += 50_000_000  # 50 ms -> half a token
    assert bucket.try_acquire() == 50
    clock.now += 50_000_000
    assert bucket.try_acquire() == 0
    clock.now += 10_000_000_000  # capped at burst
    assert bucket.tokens == 5
    with pytest.raises(ValueError, match="cannot acquire"):
        bucket.try_acquire(6)


async def test_token_bucket_acquire_waits() -> None:
    clock = FakeNs()
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.now += int(seconds * 1_000_000_000)

    bucket = TokenBucket(RateLimit(name="x", rate_per_s=D(2), burst=1), now_ns=clock, sleep=fake_sleep)
    await bucket.acquire()
    await bucket.acquire()
    assert slept == [0.5]


def test_rate_limit_validation() -> None:
    with pytest.raises(ValueError, match="> 0"):
        RateLimit(name="x", rate_per_s=D(0), burst=1)
    limits = RateLimits(buckets=(RateLimit(name="a", rate_per_s=D(1), burst=1),))
    assert limits.get("a") is not None
    assert limits.get("b") is None


def test_order_changes() -> None:
    assert OrderChanges().is_empty
    assert not OrderChanges(remove_tp=True).is_empty
    with pytest.raises(ValueError, match="remove_sl"):
        OrderChanges(sl=D(1), remove_sl=True)


# ------------------------------------------------------------------ fx


def _tick(symbol: str, bid: str, ask: str) -> Tick:
    return Tick(symbol=symbol, bid=D(bid), ask=D(ask), ts=T0, recv_ts=T0, recv_mono_ns=0)


def test_conversion_rate() -> None:
    ticks = {"EURUSD": _tick("EURUSD", "1.1000", "1.1002"), "USDJPY": _tick("USDJPY", "150.00", "150.02")}
    assert conversion_rate("USD", "USD", ticks.get) == 1
    assert conversion_rate("EUR", "USD", ticks.get) == D("1.1001")
    assert conversion_rate("JPY", "USD", ticks.get) == 1 / D("150.01")
    with pytest.raises(ConversionUnavailable):
        conversion_rate("GBP", "USD", ticks.get)


# ------------------------------------------------------------------ accounting


@given(lots, prices, prices)
def test_pnl_is_antisymmetric_and_linear(volume: Decimal, entry: Decimal, exit_price: Decimal) -> None:
    cs = D(100_000)
    long = pnl_quote(Side.BUY, volume, entry, exit_price, cs)
    short = pnl_quote(Side.SELL, volume, entry, exit_price, cs)
    assert long == -short
    assert pnl_quote(Side.BUY, volume * 2, entry, exit_price, cs) == long * 2
    assert pnl_quote(Side.BUY, volume, entry, entry, cs) == 0


@given(lots, prices, lots, prices)
def test_weighted_entry_between_inputs(v1: Decimal, p1: Decimal, v2: Decimal, p2: Decimal) -> None:
    avg = weighted_entry(v1, p1, v2, p2)
    assert min(p1, p2) - D("1e-20") <= avg <= max(p1, p2) + D("1e-20")


def test_margin_required() -> None:
    fx = eurusd()
    assert margin_required(fx, D(1), D("1.1"), 100) == (D(1000), "EUR")
    gold = eurusd(symbol="XAUUSD", contract_size=D(100), base_currency="XAU", margin_currency="USD")
    assert margin_required(gold, D(1), D(2400), 100) == (D(2400), "USD")


# ------------------------------------------------------------------ registry


def test_registry_creates_paper_and_rejects_unknown() -> None:
    clock = FixedClock(T0)
    registry = default_registry()
    assert registry.names() == {"paper"}
    account = AccountConfig.model_validate(
        {"id": "p", "broker": "paper", "paper": {"leverage": 30, "hedging": False}}
    )
    broker = registry.create(account, BrokerDeps(clock=clock, market_data=FakeTickSource([eurusd()], clock)))
    assert isinstance(broker, PaperBroker)
    assert isinstance(broker, Broker)
    assert broker.capabilities.netting

    with pytest.raises(ConfigError, match="market data"):
        registry.create(account, BrokerDeps(clock=clock))
    ct = AccountConfig.model_validate(
        {"id": "c", "broker": "ctrader", "account_id": "1", "secrets_ref": "CT"}
    )
    with pytest.raises(ConfigError, match="no adapter registered for broker 'ctrader'"):
        registry.create(ct, BrokerDeps(clock=clock))
    with pytest.raises(ValueError, match="already registered"):
        registry.register("paper", lambda a, d: broker)
    assert BrokerRegistry().names() == frozenset()


def test_paper_settings_from_config() -> None:
    account = AccountConfig.model_validate(
        {
            "id": "p",
            "broker": "paper",
            "paper": {
                "starting_balance": "5000.50",
                "commission_per_lot": "3.5",
                "rate_limit": {"rate_per_s": 5, "burst": 2},
            },
        }
    )
    settings = paper_settings(account)
    assert settings.account_id == "p"
    assert settings.starting_balance == D("5000.50")
    assert settings.commission_per_lot == D("3.5")
    assert settings.rate_limit == RateLimit(name="requests", rate_per_s=D(5), burst=2)
    assert paper_settings(AccountConfig(id="q", broker="paper")).account_id == "q"
