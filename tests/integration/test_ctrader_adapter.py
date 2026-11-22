"""cTrader transport and adapter against the in-process fake server (no network)."""

from __future__ import annotations

import asyncio
import contextlib
import os
import stat
import struct
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import SecretStr

from tests.contract.harness import EventRecorder
from tests.integration.ctrader_fake import FakeCTraderServer
from trader.brokers.base import RateLimit
from trader.brokers.ctrader import CTraderBroker, CTraderSettings, TokenSet, TokenStore
from trader.brokers.ctrader import transport as tp
from trader.brokers.ctrader.oauth import authorize_url
from trader.brokers.ctrader.proto import OpenApiCommonMessages_pb2 as common
from trader.brokers.ctrader.proto import OpenApiMessages_pb2 as msgs
from trader.brokers.registry import BrokerDeps, default_registry
from trader.config.secrets import read_env_file
from trader.config.settings import AccountConfig
from trader.domain import ConnectionEvent, ConnectionState, FillEvent, OrderType, Side, TickEvent, Timeframe
from trader.domain.errors import (
    AuthExpired,
    BrokerUnavailable,
    ConfigError,
    InvalidVolume,
    OrderNotFound,
    UnknownOrderState,
    UnknownSymbol,
)
from trader.domain.ids import new_client_order_id
from trader.domain.models import OrderRequest
from trader.domain.time import SystemClock

D = Decimal


def settings(server: FakeCTraderServer, **kw: object) -> CTraderSettings:
    fields: dict[str, object] = {
        "account_id": "ct-test",
        "ctid_trader_account_id": server.ctid,
        "environment": "demo",
        "client_id": server.client_id,
        "client_secret": SecretStr(server.client_secret),
        "symbols": ("EURUSD",),
        "host": "127.0.0.1",
        "port": server.port,
        "use_tls": False,
        "reconnect_initial_s": 0.05,
    }
    fields.update(kw)
    return CTraderSettings(**fields)  # type: ignore[arg-type]


@pytest.fixture
async def server() -> AsyncIterator[FakeCTraderServer]:
    s = FakeCTraderServer()
    await s.start()
    yield s
    await s.stop()


def make_broker(server: FakeCTraderServer, store: TokenStore | None = None, **kw: object) -> CTraderBroker:
    tokens = TokenSet(SecretStr(server.access_token), SecretStr(server.refresh_token), expires_at=None)
    return CTraderBroker(
        settings(server, **kw),
        tokens=tokens,
        store=store or TokenStore(Path("/nonexistent"), None),
        clock=SystemClock(),
    )


def market(side: Side = Side.BUY, volume: str = "0.01", **kw: object) -> OrderRequest:
    fields: dict[str, object] = {
        "client_order_id": new_client_order_id(),
        "symbol": "EURUSD",
        "side": side,
        "order_type": OrderType.MARKET,
        "volume": D(volume),
        "strategy_id": "t",
        "strategy_version": "1",
    }
    fields.update(kw)
    return OrderRequest(**fields)  # type: ignore[arg-type]


# ------------------------------------------------------------------ transport


def test_frame_encoding_is_4_byte_big_endian() -> None:
    frame = tp.encode(msgs.ProtoOAVersionReq(), "m1")
    (length,) = struct.unpack(">I", frame[:4])
    assert length == len(frame) - 4
    inbound = tp.decode(frame[4:])
    assert inbound is not None
    assert isinstance(inbound.message, msgs.ProtoOAVersionReq)
    assert inbound.client_msg_id == "m1"


def test_payload_registry_covers_core_messages() -> None:
    for cls in (
        msgs.ProtoOAExecutionEvent,
        msgs.ProtoOASpotEvent,
        msgs.ProtoOAErrorRes,
        common.ProtoHeartbeatEvent,
    ):
        assert tp.PAYLOAD_TYPES[tp.payload_type(cls())] is cls


async def test_request_timeout_is_unknown_state() -> None:
    async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read()  # never answers
        writer.close()

    srv = await asyncio.start_server(silent, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    closed: list[Exception | None] = []
    t = tp.Transport(
        "127.0.0.1", port, on_message=lambda m: None, on_closed=closed.append, request_timeout=0.2
    )
    await t.connect()
    with pytest.raises(UnknownOrderState):
        await t.request(msgs.ProtoOAVersionReq(), msgs.ProtoOAVersionRes)
    await t.close()
    srv.close()
    await srv.wait_closed()
    assert closed == [None]


async def test_idle_connection_is_closed() -> None:
    async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read()  # never answers
        writer.close()

    srv = await asyncio.start_server(silent, "127.0.0.1", 0)
    closed: list[Exception | None] = []
    t = tp.Transport(
        "127.0.0.1",
        srv.sockets[0].getsockname()[1],
        on_message=lambda m: None,
        on_closed=closed.append,
        heartbeat_s=0.05,
        idle_timeout=0.15,
    )
    await t.connect()
    await asyncio.sleep(0.4)
    assert len(closed) == 1
    assert isinstance(closed[0], BrokerUnavailable)
    srv.close()
    await srv.wait_closed()


async def test_connect_through_http_connect_proxy(server: FakeCTraderServer) -> None:
    seen: list[bytes] = []
    handlers: list[asyncio.Task[object]] = []

    async def proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        handlers.append(task)
        line = await reader.readline()
        seen.append(line)
        while (await reader.readline()) not in (b"\r\n", b""):
            pass
        up_r, up_w = await asyncio.open_connection("127.0.0.1", server.port)
        writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await writer.drain()

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
            try:
                while data := await src.read(65536):
                    dst.write(data)
                    await dst.drain()
            finally:
                dst.close()  # EOF on one side closes the other, so both pipes finish

        await asyncio.gather(pipe(reader, up_w), pipe(up_r, writer), return_exceptions=True)
        for w in (writer, up_w):
            with contextlib.suppress(Exception):
                await w.wait_closed()

    srv = await asyncio.start_server(proxy, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    broker = make_broker(server, proxy=f"http://127.0.0.1:{port}")
    await broker.connect()
    assert seen[0].startswith(f"CONNECT 127.0.0.1:{server.port} ".encode())
    assert (await broker.account()).currency == "USD"
    await broker.close()
    await asyncio.wait(handlers, timeout=5)  # let both pipes drain and close their sockets
    srv.close()
    await srv.wait_closed()


# ------------------------------------------------------------------ auth & tokens


async def test_auth_order_and_token_refresh_persisted(server: FakeCTraderServer, tmp_path: Path) -> None:
    key = SecretStr(TokenStore.generate_key())
    store = TokenStore(tmp_path / "secrets" / "ct.tokens", key)
    broker = make_broker(server, store=store)
    await broker.connect()
    assert server.received[:3] == [
        "ProtoOAApplicationAuthReq",
        "ProtoOARefreshTokenReq",
        "ProtoOAAccountAuthReq",
    ]
    saved = store.load()
    assert saved is not None
    assert saved.refresh_token.get_secret_value() == server.refresh_token  # rotated pair persisted
    assert saved.expires_at is not None
    mode = stat.S_IMODE(os.stat(tmp_path / "secrets" / "ct.tokens").st_mode)
    assert mode == 0o600
    raw = (tmp_path / "secrets" / "ct.tokens").read_bytes()
    assert server.refresh_token.encode() not in raw  # encrypted at rest
    await broker.close()

    # A fresh token set is not refreshed again on the next connect.
    fresh = TokenSet(saved.access_token, saved.refresh_token, datetime.now(UTC) + timedelta(days=29))
    again = CTraderBroker(settings(server), tokens=fresh, store=store, clock=SystemClock())
    server.received.clear()
    await again.connect()
    assert "ProtoOARefreshTokenReq" not in server.received
    await again.close()


async def test_bad_credentials_raise(server: FakeCTraderServer) -> None:
    broker = make_broker(server, client_secret=SecretStr("wrong"))
    with pytest.raises(Exception, match="CH_CLIENT_AUTH_FAILURE"):
        await broker.connect()
    tokens = TokenSet(SecretStr("bad"), SecretStr("bad"), expires_at=datetime.now(UTC) + timedelta(days=20))
    broker = CTraderBroker(
        settings(server), tokens=tokens, store=TokenStore(Path("/x"), None), clock=SystemClock()
    )
    with pytest.raises(AuthExpired):
        await broker.connect()


def test_token_store_wrong_key(tmp_path: Path) -> None:
    store = TokenStore(tmp_path / "t", SecretStr(TokenStore.generate_key()))
    store.save(TokenSet(SecretStr("a"), SecretStr("r"), None))
    with pytest.raises(ConfigError, match="decrypt"):
        TokenStore(tmp_path / "t", SecretStr(TokenStore.generate_key())).load()
    with pytest.raises(ConfigError, match="Fernet"):
        TokenStore(tmp_path / "t", SecretStr("not-a-key"))
    assert not TokenStore(tmp_path / "t", None).save(TokenSet(SecretStr("a"), SecretStr("r"), None))


def test_authorize_url() -> None:
    url = authorize_url("123_abc", "https://example.com/cb")
    assert url.startswith("https://id.ctrader.com/my/settings/openapi/grantingaccess/?")
    assert "client_id=123_abc" in url
    assert "scope=trading" in url
    assert "redirect_uri=https%3A%2F%2Fexample.com%2Fcb" in url


# ------------------------------------------------------------------ adapter behaviour


@pytest.fixture
async def connected(server: FakeCTraderServer) -> AsyncIterator[tuple[CTraderBroker, EventRecorder]]:
    broker = make_broker(server)
    await broker.connect()
    rec = EventRecorder(broker)
    await broker.subscribe_ticks(["EURUSD"])
    await rec.wait_for(TickEvent)
    await broker.snapshot()
    yield broker, rec
    await rec.stop()
    await broker.close()


async def test_instruments_only_enabled_and_requested(connected: tuple[CTraderBroker, EventRecorder]) -> None:
    broker, _ = connected
    assert [i.symbol for i in await broker.instruments()] == ["EURUSD"]
    with pytest.raises(UnknownSymbol):
        await broker.subscribe_ticks(["GBPUSD"])


async def test_market_order_with_relative_sl_tp(connected: tuple[CTraderBroker, EventRecorder]) -> None:
    broker, rec = connected
    req = market(sl=D("1.09000"), tp=D("1.12000"))
    ack = await broker.place_order(req)
    assert ack.broker_order_id is not None
    fill = (await rec.wait_for(FillEvent, lambda e: e.fill.client_order_id == req.client_order_id)).fill
    assert fill.volume == D("0.01")
    assert fill.commission == D("-0.30")
    pos = next(p for p in await broker.positions() if p.position_id == fill.position_id)
    # relative distances were computed from the reference ask and re-applied by the server
    assert pos.sl is not None
    assert abs(pos.sl - D("1.09000")) < D("0.0001")
    assert pos.tp is not None
    assert abs(pos.tp - D("1.12000")) < D("0.0001")


async def test_invalid_requests(connected: tuple[CTraderBroker, EventRecorder]) -> None:
    broker, _ = connected
    with pytest.raises(InvalidVolume):
        await broker.place_order(market(volume="0.015"))
    with pytest.raises(OrderNotFound):
        await broker.cancel_order("424242")


async def test_client_side_throttling_avoids_server_limit(
    server: FakeCTraderServer, connected: tuple[CTraderBroker, EventRecorder]
) -> None:
    broker, _ = connected
    await asyncio.gather(*(broker.account() for _ in range(30)))  # 90 requests
    assert server.rate_limited == 0


async def test_candles_shrink_window_on_incorrect_boundaries(
    connected: tuple[CTraderBroker, EventRecorder],
) -> None:
    broker, _ = connected
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=2500)  # > 1000 bars per chunk -> first chunk fine; server limit 2000
    bars = await broker.candles("EURUSD", Timeframe.M1, start, end)
    assert len(bars) == 2500
    assert bars[0].start == start
    assert all(b.open == D("1.09995") for b in bars)


async def test_reconnect_gates_trade_events_until_snapshot(
    server: FakeCTraderServer, connected: tuple[CTraderBroker, EventRecorder]
) -> None:
    broker, rec = connected
    mark = rec.mark
    await server.drop_all()
    await rec.wait_for(ConnectionEvent, lambda e: e.state is ConnectionState.DOWN, after=mark)
    await rec.wait_for(ConnectionEvent, lambda e: e.state is ConnectionState.UP, after=mark, timeout=10)
    await rec.wait_for(TickEvent, after=rec.mark)  # spots resubscribed automatically
    up_mark = rec.mark
    req = market()
    await broker.place_order(req)  # before snapshot: executes, but no trade events are emitted
    await asyncio.sleep(0.3)
    assert rec.of(FillEvent, after=up_mark) == []
    snap = await broker.snapshot()
    assert len(snap.positions) == 1  # the reconcile shows what happened
    await broker.place_order(market())
    await rec.wait_for(FillEvent, after=up_mark)


async def test_partial_bid_ask_spots_keep_last_value(server: FakeCTraderServer) -> None:
    broker = make_broker(server)
    await broker.connect()
    # Feed a bid-only spot directly through the message handler.
    broker._on_spot(msgs.ProtoOASpotEvent(ctidTraderAccountId=server.ctid, symbolId=1, bid=120_000))
    assert broker._quotes[1][0] == 120_000
    assert broker._quotes[1][1] is None  # no ask yet -> no tick emitted
    broker._on_spot(msgs.ProtoOASpotEvent(ctidTraderAccountId=server.ctid, symbolId=1, ask=120_010))
    assert broker._quotes[1] == (120_000, 120_010)
    await broker.close()


async def test_requests_when_not_connected(server: FakeCTraderServer) -> None:
    broker = make_broker(server)
    with pytest.raises(BrokerUnavailable):
        await broker.account()


# ------------------------------------------------------------------ registry / config


def test_registry_builds_ctrader_from_env(tmp_path: Path) -> None:
    account = AccountConfig.model_validate(
        {
            "id": "ct",
            "broker": "ctrader",
            "account_id": "1001",
            "secrets_ref": "CT_DEMO",
            "symbols": ["EURUSD"],
        }
    )
    env = {"CT_DEMO_CLIENT_ID": "id", "CT_DEMO_CLIENT_SECRET": "s", "CT_DEMO_ACCESS_TOKEN": "a"}
    deps = BrokerDeps(clock=SystemClock(), env=env, data_dir=tmp_path)
    with pytest.raises(ConfigError, match="CT_DEMO_REFRESH_TOKEN"):
        default_registry().create(account, deps)
    broker = default_registry().create(
        account, BrokerDeps(clock=SystemClock(), env={**env, "CT_DEMO_REFRESH_TOKEN": "r"}, data_dir=tmp_path)
    )
    assert isinstance(broker, CTraderBroker)
    bad = AccountConfig.model_validate(
        {"id": "x", "broker": "ctrader", "account_id": "abc", "secrets_ref": "X_REF"}
    )
    with pytest.raises(ConfigError, match="ctidTraderAccountId"):
        default_registry().create(bad, deps)


def test_read_env_file(tmp_path: Path) -> None:
    f = tmp_path / ".env"
    f.write_text('# comment\nA=1\nexport B="two words"\nC=3 # trailing\n\nBAD LINE\n')
    assert read_env_file(f) == {"A": "1", "B": "two words", "C": "3"}
    assert read_env_file(tmp_path / "missing") == {}


async def test_token_refreshed_while_connected(server: FakeCTraderServer, tmp_path: Path) -> None:
    store = TokenStore(tmp_path / "t", SecretStr(TokenStore.generate_key()))
    soon = TokenSet(
        SecretStr(server.access_token),
        SecretStr(server.refresh_token),
        datetime.now(UTC) + timedelta(days=30),
    )
    broker = CTraderBroker(
        settings(server, account_poll_s=0.05, token_refresh_margin=timedelta(days=7)),
        tokens=soon, store=store, clock=SystemClock(),
    )  # fmt: skip
    await broker.connect()
    assert server.refreshes == 0  # 30 days left: no refresh at connect
    broker._tokens = TokenSet(soon.access_token, soon.refresh_token, datetime.now(UTC) + timedelta(days=1))
    for _ in range(50):
        if server.refreshes:
            break
        await asyncio.sleep(0.02)
    assert server.refreshes == 1
    saved = store.load()
    assert saved is not None
    assert saved.refresh_token.get_secret_value() == server.refresh_token
    await broker.close()


async def test_find_order_bisects_truncated_history(
    connected: tuple[CTraderBroker, EventRecorder], server: FakeCTraderServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker, rec = connected
    since = datetime.now(UTC) - timedelta(seconds=1)
    reqs = [market() for _ in range(3)]
    for r in reqs:
        await broker.place_order(r)
        await rec.wait_for(FillEvent, lambda e, r=r: e.fill.client_order_id == r.client_order_id)
        await asyncio.sleep(0.02)  # distinct update timestamps
    server.history_chunk = 1  # every window holding more than one finished order is truncated
    # Speed up the client-side historical throttle for this test only (the fake allows 50 req/s in all).
    monkeypatch.setattr(tp, "_CLIENT_HISTORICAL", RateLimit(name="historical", rate_per_s=D(25), burst=5))
    fresh = make_broker(server)  # no local client-id links: the lookup must come from history
    await fresh.connect()
    try:
        before = server.history_requests
        for r in (reqs[0], reqs[-1]):
            found = await fresh.find_order(r.client_order_id, since)
            assert found is not None
            assert found.status.value == "FILLED"
        assert server.history_requests - before > 2  # windows were split
        assert server.rate_limited == 0
    finally:
        await fresh.close()
