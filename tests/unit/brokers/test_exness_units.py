"""Exness pure pieces: signing (official test vector), key formats, limiter, mapping."""

from __future__ import annotations

from datetime import UTC, datetime, time
from decimal import Decimal

import pytest
from pydantic import SecretStr

from trader.brokers.exness import mapping as mp
from trader.brokers.exness.auth import Signer, body_hash, is_valid_idempotency_key, load_signing_key
from trader.brokers.exness.rest import RequestLimits, base_url, build_query
from trader.brokers.ratelimit import SlidingWindow
from trader.domain import OrderStatus, OrderType, Side, Timeframe
from trader.domain.errors import (
    AuthExpired,
    BrokerError,
    ConfigError,
    InsufficientMargin,
    InvalidPrice,
    InvalidVolume,
    MarketClosed,
    RateLimited,
    Requote,
)

D = Decimal

# Official synthetic vector: https://www.exness-api.com/trading/guides/code-examples (RFC 8032 test 1 key).
VECTOR_HEX = (
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
)
VECTOR_PEM = (
    "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIJ1hsZ3v/VpguoRK9JLsLMREScVpezJpGXA7rAMcrn9g\n"
    "-----END PRIVATE KEY-----"
)
VECTOR_DATA = (
    "eyJhcGlfa2V5IjoiZXhuc2stc3ludGhldGljLWV4YW1wbGUiLCJpZGVtcG90ZW5jeV9rZXkiOiIiLCJ0aW1lc3RhbXAiOjE3NzM2NTYwNzAx"
    "MjMsInNpZ25fdmVyc2lvbiI6MSwibWV0aG9kIjoiR0VUIiwicGF0aCI6Ii92MS90cmFkaW5nL2FjY2Vzcy1wb2ludD9hY2NvdW50X2lkPTEy"
    "MzQ1NiIsImJvZHlfaGFzaCI6IjQ3REVRcGo4SEJTYS1fVEltVy01SkNldVFlUmttNU5NcEpXWkczaFN1RlUifQ"
)
VECTOR_SIGN = "l-H_Acia8YoTjKFMg-3fon3NOEX8TalNVA7c01DQ9dHb7pVBJC4y06qC-DbpETzh1KToiOprqEV6uzN3t69ICQ"


@pytest.mark.parametrize("key", [VECTOR_HEX, VECTOR_PEM, VECTOR_PEM.replace("\n", "\\n")])
def test_signer_matches_official_vector(key: str) -> None:
    signer = Signer("exnsk-synthetic-example", load_signing_key(SecretStr(key)), now_ms=lambda: 1773656070123)
    headers = signer.headers("GET", "/v1/trading/access-point?account_id=123456")
    assert headers == {
        "EXN-API-KEY": "exnsk-synthetic-example",
        "EXN-IDEMPOTENCY-KEY": "",
        "EXN-TIMESTAMP": "1773656070123",
        "EXN-SIGN-VERSION": "1",
        "EXN-DATA": VECTOR_DATA,
        "EXN-SIGN": VECTOR_SIGN,
    }


def test_empty_body_hash_and_rules() -> None:
    assert body_hash(b"") == "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU"
    signer = Signer("k", load_signing_key(SecretStr(VECTOR_HEX)))
    with pytest.raises(ValueError, match="empty idempotency"):
        signer.headers("GET", "/x", idempotency_key="abcde")
    with pytest.raises(ValueError, match="idempotency key"):
        signer.headers("POST", "/x", b"{}", idempotency_key="")
    with pytest.raises(ValueError, match="not a full URL"):
        signer.headers("GET", "https://host/x")
    post = signer.headers("POST", "/x", b'{"a":1}', idempotency_key="01JABCDEF")
    assert "=" not in post["EXN-DATA"]
    assert "=" not in post["EXN-SIGN"]
    assert is_valid_idempotency_key("01M81EAAK1D1RKQMBS0REPVSRC")
    assert not is_valid_idempotency_key("abc")
    assert not is_valid_idempotency_key("has space!")


def test_bad_keys_never_echo_material() -> None:
    with pytest.raises(ConfigError, match="128 hex") as info:
        load_signing_key(SecretStr("deadbeef"))
    assert "deadbeef" not in str(info.value)
    tampered = VECTOR_HEX[:-2] + ("00" if VECTOR_HEX[-2:] != "00" else "11")
    with pytest.raises(ConfigError, match="public half"):
        load_signing_key(SecretStr(tampered))
    with pytest.raises(ConfigError, match="invalid PEM"):
        load_signing_key(SecretStr("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----"))


# ------------------------------------------------------------------ limiter / REST helpers


class Clock:
    def __init__(self) -> None:
        self.ns = 0

    def __call__(self) -> int:
        return self.ns


def test_sliding_window_is_exact() -> None:
    clock = Clock()
    win = SlidingWindow(3, D(1), now_ns=clock)
    assert [win.try_acquire() for _ in range(3)] == [0, 0, 0]
    assert win.try_acquire() == 1000
    clock.ns = 400_000_000
    assert win.try_acquire() == 600
    clock.ns = 1_000_000_000
    assert win.try_acquire() == 0  # the first three expired exactly at 1 s
    with pytest.raises(ValueError, match="> 0"):
        SlidingWindow(0, D(1))


def test_request_limits_from_payload() -> None:
    limits = RequestLimits()
    limits.configure(
        {
            "limits": {
                "rest": {
                    "global_account_rate": {"limit": 100, "window_seconds": 60},
                    "methods": [
                        {
                            "api_operation_id": "openPosition",
                            "http_method": "POST",
                            "path": "/p",
                            "rate_limit": {"limit": 10, "window_seconds": 1},
                        },
                        {
                            "api_operation_id": "getCandleHistory",
                            "http_method": "GET",
                            "path": "/c",
                            "rate_limit": {"limit": 5, "window_seconds": 1},
                            "limits": {"max_candles_per_request": 500},
                        },
                        {
                            "api_operation_id": "getDealsHistory",
                            "http_method": "GET",
                            "path": "/d",
                            "rate_limit": {"limit": 5, "window_seconds": 1},
                            "limits": {"history_range_days": 31, "history_age_days": 365},
                        },
                    ],
                },
                "websocket": {"global_account_limits": {}, "endpoints": []},
            }
        }
    )
    assert limits.candles_per_request == 500
    assert (limits.history_range_days, limits.history_age_days) == (31, 365)
    published = limits.published()
    account = published.get("account")
    assert account is not None
    assert account.burst == 100
    assert account.rate_per_s == D(100) / 60
    assert published.get("openPosition") is not None


def test_query_and_base_url() -> None:
    assert build_query([("from", "2026-01-05T08:00:00.000Z"), ("instrument", "EUR/USD")]) == (
        "from=2026-01-05T08%3A00%3A00.000Z&instrument=EUR%2FUSD"
    )
    with pytest.raises(ValueError, match="duplicate"):
        build_query([("a", "1"), ("a", "2")])
    assert base_url("https://ap-1.example/") == "https://ap-1.example"
    for bad in ("http://ap.example", "https://user:pw@ap.example", "https://ap.example/v1", "ftp://x"):
        with pytest.raises(ConfigError):
            base_url(bad)
    assert base_url("http://127.0.0.1:9", allow_insecure=True) == "http://127.0.0.1:9"


# ------------------------------------------------------------------ mapping


@pytest.mark.parametrize(
    ("code", "cls"),
    [
        ("EXECUTION_REQUOTE", Requote),
        ("TRADING_RULE_INSUFFICIENT_MARGIN", InsufficientMargin),
        ("TRADING_RULE_INVALID_VOLUME_STEP", InvalidVolume),
        ("TRADING_RULE_INVALID_PRICE_LEVELS", InvalidPrice),
        ("MARKET_SESSION_CLOSED", MarketClosed),
        ("AUTH_INVALID_SIGNATURE", AuthExpired),
        ("SOMETHING_NEW", BrokerError),
    ],
)
def test_error_mapping(code: str, cls: type[BrokerError]) -> None:
    err = mp.map_error(code)
    assert type(err) is cls
    assert err.broker_code == code


def test_rate_limit_error() -> None:
    err = mp.map_error("REQUEST_RATE_LIMIT", retry_after_ms=2500)
    assert isinstance(err, RateLimited)
    assert err.retry_after_ms == 2500


def test_sessions_inclusive_end_and_week_wrap() -> None:
    s = mp.sessions_from_wire([{"week_start_time": "Mon, 10:00:00", "week_end_time": "Mon, 11:59:59"}])
    assert [(x.weekday, x.start, x.end) for x in s] == [(0, time(10), time(12))]
    week = mp.sessions_from_wire([{"week_start_time": "Sun, 22:05:00", "week_end_time": "Fri, 20:54:59"}])
    assert week[0].weekday == 6
    assert (week[0].start, week[0].end) == (time(22, 5), time(0))
    assert week[-1].weekday == 4
    assert week[-1].end == time(20, 55)
    disabled = mp.sessions_from_wire(
        [
            {
                "trade_mode": "trading_disabled",
                "week_start_time": "Mon, 00:00:00",
                "week_end_time": "Mon, 01:00:00",
            }
        ]
    )
    assert disabled == ()


CONDITIONS = {
    "instrument": "XAUUSDm",
    "base_currency": "XAU",
    "quote_currency": "USD",
    "margin_currency": "USD",
    "point_digits": 3,
    "exec_mode": "market",
    "contract_size": "100",
    "volume_min": "0.01",
    "volume_max": "200",
    "volume_step": "0.01",
    "order_types": ["market", "limit", "stop"],
}


def test_instrument_from_conditions_and_suffix() -> None:
    info = mp.instrument_from_wire(CONDITIONS, suffix="m", aliases={}, hedging=True)
    inst = info.instrument
    assert (inst.symbol, inst.broker_symbol, inst.digits) == ("XAUUSD", "XAUUSDm", 3)
    assert inst.tick_size == D("0.001")
    assert inst.contract_size == D(100)
    assert inst.margin_currency == "USD"
    assert mp.canonical_symbol("GOLDm", suffix="m", aliases={"GOLDm": "XAUUSD"}) == "XAUUSD"
    with pytest.raises(ValueError, match="contract_size"):
        mp.instrument_from_wire({**CONDITIONS, "contract_size": None}, suffix="", aliases={}, hedging=True)


def test_prices_volumes_times() -> None:
    assert mp.price_str(D("1.10010"), 5) == "1.10010"
    with pytest.raises(InvalidPrice):
        mp.price_str(D("1.100101"), 5)
    assert mp.volume_str(D("0.10")) == "0.10"
    assert mp.volume_str(D(2)) == "2"
    assert mp.parse_time("2026-03-16T12:54:30+00:00") == datetime(2026, 3, 16, 12, 54, 30, tzinfo=UTC)
    assert mp.iso(datetime(2026, 1, 5, 8, 0, 0, 123456, tzinfo=UTC)) == "2026-01-05T08:00:00.123Z"
    assert mp.dec(1.1) == D("1.1")
    with pytest.raises(ValueError, match="expected a number"):
        mp.dec(None)
    assert Timeframe.MN1 in mp.TIMEFRAMES
    assert mp.TIMEFRAMES[Timeframe.MN1] == "MN"


def test_tick_and_order_mapping() -> None:
    now = datetime(2026, 1, 5, tzinfo=UTC)
    info = mp.instrument_from_wire(
        {**CONDITIONS, "instrument": "EURUSDm", "point_digits": 5, "contract_size": "100000"},
        suffix="m",
        aliases={},
        hedging=True,
    )
    tick = mp.tick_from_wire(
        {"instrument": "EURUSDm", "bid": 1.1, "ask": 1.10012, "timestamp": "2026-01-05T00:00:01+00:00"},
        "EURUSD",
        5,
        now=now,
        mono_ns=1,
    )
    assert (tick.bid, tick.ask) == (D("1.10000"), D("1.10012"))
    assert tick.broker_ts == datetime(2026, 1, 5, 0, 0, 1, tzinfo=UTC)
    o = {
        "order_id": "1001",
        "order_type": "limit",
        "state": "placed",
        "direction": "buy",
        "instrument": "EURUSDm",
        "price": "1.0800",
        "volume": "1.0",
        "stop_loss_price": "1.0750",
        "create_time": "2026-12-14T11:46:30+00:00",
    }
    order = mp.order_from_wire(o, info, mp.Tag("01M81EAAK1D1RKQMBS0REPVSRC", "s1", "2"))
    assert order.status is OrderStatus.WORKING
    assert order.request.order_type is OrderType.LIMIT
    assert order.request.limit_price == D("1.08000")
    assert order.request.sl == D("1.0750")
    assert (order.request.client_order_id, order.request.strategy_id) == ("01M81EAAK1D1RKQMBS0REPVSRC", "s1")
    orphan = mp.order_from_wire(
        {**o, "order_type": "market", "state": "filled", "price": "1.0834"}, info, None
    )
    assert orphan.request.client_order_id == "exness-1001"
    assert orphan.request.strategy_id == "manual"
    assert orphan.status is OrderStatus.FILLED
    assert orphan.avg_fill_price == D("1.08340")
    pos = mp.position_from_wire(
        {
            "position_id": "2001",
            "direction": "sell",
            "instrument": "EURUSDm",
            "volume": "0.5",
            "open_price": "1.0834",
            "state": "open",
            "create_time": "2026-12-14T11:46:30+00:00",
            "swap": "-1.20",
            "commission": "-0.30",
        },
        info,
        strategy_id="s1",
        sl=D("1.09"),
        tp=None,
    )
    assert (pos.side, pos.volume, pos.entry_price, pos.swap, pos.sl) == (
        Side.SELL,
        D("0.5"),
        D("1.08340"),
        D("-1.20"),
        D("1.09"),
    )
