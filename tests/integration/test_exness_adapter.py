"""Exness REST client and adapter against the in-process fake (no network)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from tests.contract.harness import EventRecorder
from tests.integration.exness_fake import ACCOUNT, API_KEY, FakeExness
from trader.brokers.base import OrderChanges
from trader.brokers.exness import ExnessBroker, ExnessSettings
from trader.brokers.exness.auth import Signer, load_signing_key
from trader.brokers.exness.rest import RequestLimits, RestClient
from trader.brokers.registry import BrokerDeps, default_registry
from trader.config.settings import AccountConfig
from trader.domain import (
    AccountEvent,
    ConnectionEvent,
    ConnectionState,
    FillEvent,
    OrderEvent,
    OrderStatus,
    OrderType,
    PositionChange,
    PositionEvent,
    Side,
    TickEvent,
    Timeframe,
    TimeInForce,
)
from trader.domain.errors import (
    AuthExpired,
    BrokerError,
    BrokerUnavailable,
    ConfigError,
    InvalidVolume,
    OrderNotFound,
    OrderRejected,
    RateLimited,
    UnknownOrderState,
)
from trader.domain.ids import new_client_order_id
from trader.domain.models import OrderRequest
from trader.domain.time import SystemClock

D = Decimal
KEY_HEX = (
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
)


def settings(fake: FakeExness, **kw: object) -> ExnessSettings:
    fields: dict[str, object] = {
        "account_id": "ex-test",
        "exness_account_id": ACCOUNT,
        "gateway_url": "http://gateway.test",
        "api_key": API_KEY,
        "private_key": SecretStr(fake.private_key_hex),
        "symbols": ("EURUSD",),
        "symbol_suffix": "m",
        "allow_insecure": True,
        "trust_env": False,
        "reconnect_initial_s": 0.05,
        "ws_subprotocol": None,
        "operation_timeout_s": 0.5,
    }
    fields.update(kw)
    return ExnessSettings(**fields)  # type: ignore[arg-type]


def req(side: Side = Side.BUY, volume: str = "0.01", **kw: object) -> OrderRequest:
    fields: dict[str, object] = {
        "client_order_id": new_client_order_id(),
        "symbol": "EURUSD",
        "side": side,
        "order_type": OrderType.MARKET,
        "volume": D(volume),
        "strategy_id": "ema_cross_eurusd_m5_long_name",
        "strategy_version": "3",
    }
    fields.update(kw)
    return OrderRequest(**fields)  # type: ignore[arg-type]


@pytest.fixture
async def fake() -> AsyncIterator[FakeExness]:
    f = FakeExness()
    await f.start()
    yield f
    await f.stop()


@pytest.fixture
async def live(fake: FakeExness) -> AsyncIterator[tuple[ExnessBroker, EventRecorder]]:
    broker = ExnessBroker(settings(fake), clock=SystemClock(), http_transport=fake.transport)
    await broker.connect()
    rec = EventRecorder(broker)
    await broker.subscribe_ticks(["EURUSD"])
    await rec.wait_for(TickEvent)
    await broker.snapshot()
    yield broker, rec
    await rec.stop()
    await broker.close()


# ------------------------------------------------------------------ REST client


def _client(handler: object) -> tuple[RestClient, httpx.AsyncClient]:
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]
    signer = Signer("k", load_signing_key(SecretStr(KEY_HEX)))
    return RestClient("https://ap.example", signer, http, RequestLimits()), http


async def test_rest_error_shapes() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/json-error":
            return httpx.Response(
                422, json={"code": 5006, "error_message": "TRADING_RULE_INSUFFICIENT_MARGIN"}
            )
        if path == "/rate":
            return httpx.Response(
                429, headers={"Retry-After": "2"}, json={"code": 3015, "error_message": "REQUEST_RATE_LIMIT"}
            )
        if path == "/html":
            return httpx.Response(403, headers={"content-type": "text/html"}, text="<html>denied</html>")
        if path == "/redirect":
            return httpx.Response(302, headers={"Location": "https://elsewhere.example/steal"})
        return httpx.Response(503, text="busy")

    rest, http = _client(handler)
    from trader.domain.errors import InsufficientMargin

    with pytest.raises(InsufficientMargin):
        await rest.get("x", "/json-error")
    with pytest.raises(RateLimited) as info:
        await rest.get("x", "/rate")
    assert info.value.retry_after_ms == 2000
    with pytest.raises(BrokerError, match="text/html"):
        await rest.get("x", "/html")
    with pytest.raises(BrokerError, match="302"):  # never follows redirects with signed headers
        await rest.get("x", "/redirect")
    with pytest.raises(BrokerUnavailable):
        await rest.get("x", "/busy")
    await http.aclose()


async def test_timeouts_mutating_unknown_reads_unavailable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    rest, http = _client(handler)
    with pytest.raises(UnknownOrderState):
        await rest.send(
            "openPosition", "POST", "/p", idempotency_key="01M81EAAK1D1RKQMBS0REPVSRC", body={"a": 1}
        )
    with pytest.raises(BrokerUnavailable):
        await rest.get("getAccountDetails", "/a")
    await http.aclose()


# ------------------------------------------------------------------ connect / auth


async def test_connect_sequence_and_signatures(fake: FakeExness) -> None:
    broker = ExnessBroker(settings(fake), clock=SystemClock(), http_transport=fake.transport)
    await broker.connect()
    assert fake.requests[:3] == [
        "GET /v1/trading/access-point",
        f"GET /v1/configuration/accounts/{ACCOUNT}/account",
        f"GET /v1/configuration/accounts/{ACCOUNT}/limits",
    ]
    assert fake.auth_failures == 0  # every REST call and WS handshake verified by the fake
    assert [i.symbol for i in await broker.instruments()] == ["EURUSD"]  # BTCUSDm not requested
    assert broker.capabilities.max_candles_per_request == 1000
    limits = await broker.limits()
    assert limits.get("account") is not None
    await broker.close()


async def test_wrong_key_is_rejected(fake: FakeExness) -> None:
    other = (
        "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb"
        "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c"
    )
    broker = ExnessBroker(
        settings(fake, private_key=SecretStr(other)), clock=SystemClock(), http_transport=fake.transport
    )
    with pytest.raises(AuthExpired, match="AUTH_INVALID_SIGNATURE"):
        await broker.connect()
    await broker.close()


# ------------------------------------------------------------------ trading


async def test_market_order_events_and_tagging(live: tuple[ExnessBroker, EventRecorder]) -> None:
    broker, rec = live
    r = req(sl=D("1.09000"), tp=D("1.12000"))
    ack = await broker.place_order(r)
    assert ack.client_order_id == r.client_order_id
    statuses = [OrderStatus.ACCEPTED, OrderStatus.FILLED]
    fill = (await rec.wait_for(FillEvent, lambda e: e.fill.client_order_id == r.client_order_id)).fill
    await rec.wait_for(
        OrderEvent,
        lambda e: e.order.client_order_id == r.client_order_id and e.order.status is OrderStatus.FILLED,
    )
    seen = [e.order.status for e in rec.of(OrderEvent) if e.order.client_order_id == r.client_order_id]
    assert seen == statuses
    assert fill.commission == D("-0.30")
    pos = next(p for p in await broker.positions() if p.position_id == fill.position_id)
    assert pos.strategy_id == "ema_cross_eurusd_m5_long_name"  # attributed locally (Exness echoes no comment)
    assert (pos.sl, pos.tp) == (D("1.09000"), D("1.12000"))  # from the linked sltp order
    await rec.wait_for(AccountEvent)


async def test_sltp_lifecycle_on_position(live: tuple[ExnessBroker, EventRecorder], fake: FakeExness) -> None:
    broker, rec = live
    r = req()
    await broker.place_order(r)
    pid = (
        await rec.wait_for(FillEvent, lambda e: e.fill.client_order_id == r.client_order_id)
    ).fill.position_id
    mark = rec.mark
    await broker.modify_position(pid, D("1.09000"), None)  # no sltp yet -> POST type=sltp
    await rec.wait_for(PositionEvent, lambda e: e.position.sl == D("1.09000"), after=mark)
    mark = rec.mark
    await broker.modify_position(pid, D("1.09500"), D("1.11000"))  # existing -> PATCH
    await rec.wait_for(PositionEvent, lambda e: e.position.tp == D("1.11000"), after=mark)
    mark = rec.mark
    await broker.modify_position(pid, None, None)  # both None -> cancel the sltp order
    await rec.wait_for(PositionEvent, lambda e: e.position.sl is None and e.position.tp is None, after=mark)
    calls = [c for c in fake.requests if "/orders" in c]
    assert [c.split()[0] for c in calls[:3]] == ["POST", "PATCH", "DELETE"]


async def test_pending_order_modify_and_unsupported(live: tuple[ExnessBroker, EventRecorder]) -> None:
    broker, rec = live
    r = req(order_type=OrderType.LIMIT, limit_price=D("1.05000"), sl=D("1.04000"))
    await broker.place_order(r)
    working = await rec.wait_for(
        OrderEvent,
        lambda e: e.order.client_order_id == r.client_order_id and e.order.status is OrderStatus.WORKING,
    )
    oid = working.order.broker_order_id
    assert oid is not None
    assert working.order.request.sl == D("1.04000")
    await broker.modify_order(oid, OrderChanges(remove_sl=True, limit_price=D("1.04900")))
    changed = await rec.wait_for(
        OrderEvent, lambda e: e.order.broker_order_id == oid and e.order.request.limit_price == D("1.04900")
    )
    assert changed.order.request.sl is None
    with pytest.raises(OrderRejected, match="only price"):
        await broker.modify_order(oid, OrderChanges(volume=D("0.02")))
    with pytest.raises(OrderRejected, match="stop-limit"):
        await broker.place_order(
            req(order_type=OrderType.STOP_LIMIT, stop_price=D("1.2"), limit_price=D("1.21"))
        )
    with pytest.raises(OrderRejected, match="expiry"):
        await broker.place_order(
            req(
                order_type=OrderType.LIMIT,
                limit_price=D("1.05"),
                time_in_force=TimeInForce.GTD,
                expiry=datetime.now(UTC) + timedelta(hours=1),
            )
        )
    with pytest.raises(InvalidVolume):
        await broker.place_order(req(volume="0.015"))
    with pytest.raises(OrderNotFound):
        await broker.modify_order("999999", OrderChanges(limit_price=D("1.0")))


async def test_async_rejection_becomes_rejected_event(live: tuple[ExnessBroker, EventRecorder]) -> None:
    broker, rec = live
    r = req(
        order_type=OrderType.LIMIT, limit_price=D("1.20000")
    )  # buy limit above market -> rejected after ACK
    ack = await broker.place_order(r)
    assert not ack.duplicate
    rejected = await rec.wait_for(
        OrderEvent,
        lambda e: e.order.client_order_id == r.client_order_id and e.order.status is OrderStatus.REJECTED,
    )
    assert rejected.order.rejection_reason == "TRADING_RULE_INVALID_PRICE_LEVELS"


async def test_missed_transaction_is_recovered(
    live: tuple[ExnessBroker, EventRecorder], fake: FakeExness
) -> None:
    broker, rec = live
    fake.drop_next_transaction = True
    mark = rec.mark
    r = req()
    await broker.place_order(r)
    # The watchdog polls the operation (confirmed), resubscribes, and the new snapshot is the baseline.
    await rec.wait_for(ConnectionEvent, lambda e: e.state is ConnectionState.DOWN, after=mark, timeout=10)
    await rec.wait_for(ConnectionEvent, lambda e: e.state is ConnectionState.UP, after=mark, timeout=10)
    assert any("/operations/" in c for c in fake.requests)
    snap = await broker.snapshot()
    assert len(snap.positions) == 1


async def test_server_side_idempotency(live: tuple[ExnessBroker, EventRecorder], fake: FakeExness) -> None:
    broker, rec = live
    r = req()
    await broker.place_order(r)
    await rec.wait_for(FillEvent, lambda e: e.fill.client_order_id == r.client_order_id)
    again = await broker.place_order(r)
    assert again.duplicate
    await asyncio.sleep(0.2)
    assert len(fake.positions) == 1


async def test_candles_are_chunked_by_published_limit(
    live: tuple[ExnessBroker, EventRecorder], fake: FakeExness
) -> None:
    broker, _ = live
    end = datetime(2026, 1, 5, 12, tzinfo=UTC)
    bars = await broker.candles("EURUSD", Timeframe.M1, end - timedelta(minutes=2500), end)
    assert len(bars) == 2500
    assert sum(1 for c in fake.requests if c.endswith("/candles")) == 3
    assert bars[0].open == D("1.09995")


async def test_client_throttling_avoids_server_limit(
    live: tuple[ExnessBroker, EventRecorder], fake: FakeExness
) -> None:
    broker, _ = live
    await asyncio.gather(*(broker.account() for _ in range(60)))
    assert fake.rate_limited == 0


async def test_ticks_stream_loss_is_degraded_not_down(
    live: tuple[ExnessBroker, EventRecorder], fake: FakeExness
) -> None:
    _broker, rec = live
    mark = rec.mark
    for c in [c for c in fake._clients if c.kind == "ticks"]:
        await c.ws.close()
    await rec.wait_for(ConnectionEvent, lambda e: e.state is ConnectionState.DEGRADED, after=mark)
    await rec.wait_for(ConnectionEvent, lambda e: e.state is ConnectionState.UP, after=mark, timeout=10)
    assert not any(
        isinstance(e, ConnectionEvent) and e.state is ConnectionState.DOWN for e in rec.events[mark:]
    )
    await rec.wait_for(TickEvent, after=rec.mark)


async def test_partial_close(live: tuple[ExnessBroker, EventRecorder]) -> None:
    broker, rec = live
    r = req(volume="0.05")
    await broker.place_order(r)
    pid = (
        await rec.wait_for(FillEvent, lambda e: e.fill.client_order_id == r.client_order_id)
    ).fill.position_id
    await broker.close_position(pid, D("0.02"))
    ev = await rec.wait_for(PositionEvent, lambda e: e.change is PositionChange.PARTIALLY_CLOSED)
    assert ev.position.volume == D("0.03")
    close = await rec.wait_for(
        FillEvent, lambda e: e.fill.position_id == pid and e.fill.realized_pnl is not None
    )
    assert close.fill.client_order_id is None
    assert close.fill.side is Side.SELL
    with pytest.raises(InvalidVolume):
        await broker.close_position(pid, D("1"))


# ------------------------------------------------------------------ registry


def test_registry_builds_exness(tmp_path: Path) -> None:
    base = {
        "id": "ex",
        "broker": "exness",
        "account_id": "70001234",
        "secrets_ref": "EX_DEMO",
        "symbols": ["EURUSD"],
    }
    no_gateway = AccountConfig.model_validate(base)
    with pytest.raises(ConfigError, match="gateway_url"):
        default_registry().create(no_gateway, BrokerDeps(clock=SystemClock(), env={"EX_DEMO_API_KEY": "k"}))
    account = AccountConfig.model_validate({**base, "exness": {"gateway_url": "https://gw.example"}})
    with pytest.raises(ConfigError, match="PRIVATE_KEY"):
        default_registry().create(account, BrokerDeps(clock=SystemClock(), env={"EX_DEMO_API_KEY": "k"}))
    key_file = tmp_path / "private.pem"
    key_file.write_text(KEY_HEX)
    key_file.chmod(0o644)
    env = {
        "EX_DEMO_API_KEY": "k",
        "EX_DEMO_PRIVATE_KEY_FILE": str(key_file),
        "EX_DEMO_CONFIRM_DEMO_ACCOUNT": "yes",
    }
    with pytest.raises(ConfigError, match="mode 0644: run `chmod 600"):
        default_registry().create(account, BrokerDeps(clock=SystemClock(), env=env))  # others can read it
    key_file.chmod(0o600)
    del env["EX_DEMO_CONFIRM_DEMO_ACCOUNT"]
    # A demo-labelled Exness account needs the explicit confirmation (one host for demo and real).
    with pytest.raises(ConfigError, match="EX_DEMO_CONFIRM_DEMO_ACCOUNT=yes"):
        default_registry().create(account, BrokerDeps(clock=SystemClock(), env=env))
    env["EX_DEMO_CONFIRM_DEMO_ACCOUNT"] = "yes"
    assert isinstance(
        default_registry().create(account, BrokerDeps(clock=SystemClock(), env=env)), ExnessBroker
    )
    with pytest.raises(ValueError, match="gateway_url"):
        AccountConfig.model_validate({**base, "exness": {"gateway_url": "http://insecure.example"}})


async def _watchdog_resubscribed(rec: EventRecorder, mark: int) -> None:
    """No transaction_event follows a de-duplicated resend: the watchdog polls the operation, tries to
    re-link it, then drops and re-establishes the stream."""
    down = await rec.wait_for(
        ConnectionEvent, lambda e: e.state is ConnectionState.DOWN, after=mark, timeout=10
    )
    await rec.wait_for(
        ConnectionEvent, lambda e: e.state is ConnectionState.UP, after=rec.events.index(down), timeout=10
    )


async def _fresh(fake: FakeExness) -> tuple[ExnessBroker, EventRecorder]:
    broker = ExnessBroker(settings(fake), clock=SystemClock(), http_transport=fake.transport)
    await broker.connect()
    rec = EventRecorder(broker)
    await broker.snapshot()
    return broker, rec


async def test_restarted_adapter_relinks_a_deduplicated_pending_order(fake: FakeExness) -> None:
    since = datetime.now(UTC) - timedelta(minutes=1)
    r = req(order_type=OrderType.LIMIT, limit_price=D("1.09000"))
    first, rec = await _fresh(fake)
    await first.place_order(r)
    placed = await rec.wait_for(OrderEvent, lambda e: e.order.status is OrderStatus.WORKING)
    await rec.stop()
    await first.close()

    broker, rec = await _fresh(fake)  # a new process: no client-id links
    try:
        assert await broker.find_order(r.client_order_id, since) is None
        mark = rec.mark
        await broker.place_order(r)  # same key: the server returns the original operation
        await _watchdog_resubscribed(rec, mark)
        found = await broker.find_order(r.client_order_id, since)
        assert found is not None
        assert found.status is OrderStatus.WORKING
        assert found.broker_order_id == placed.order.broker_order_id
        assert len(fake.orders) == 1
    finally:
        await rec.stop()
        await broker.close()


async def test_restarted_adapter_refuses_an_ambiguous_relink(fake: FakeExness) -> None:
    since = datetime.now(UTC) - timedelta(minutes=1)
    first, rec = await _fresh(fake)
    r1, r2 = req(volume="0.02"), req(volume="0.02")  # identical content, different keys
    for r in (r1, r2):
        await first.place_order(r)
        await rec.wait_for(FillEvent, lambda e, r=r: e.fill.client_order_id == r.client_order_id)
    await rec.stop()
    await first.close()

    broker, rec = await _fresh(fake)
    try:
        mark = rec.mark
        await broker.place_order(r1)
        await _watchdog_resubscribed(rec, mark)
        # Two open deals match: linking either could attribute the wrong one, so neither is linked.
        assert await broker.find_order(r1.client_order_id, since) is None
        assert len(fake.positions) == 2
    finally:
        await rec.stop()
        await broker.close()
