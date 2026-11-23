"""Phase 10 acceptance: API contract tests. The real ``App`` on the paper broker behind uvicorn, called over
HTTP: auth on every route, response shapes (validated against the response models, which are the OpenAPI
contract), manual orders through risk, kill switch, strategy lifecycle, errors, audit."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from pydantic import TypeAdapter

from tests.integration.api_helpers import ACCOUNT, TOKEN, Running, api_config, running_app, wait_for
from trader.api import schemas as s
from trader.api.routes import openapi_schema
from trader.domain.enums import KillLevel
from trader.strategy.base import StrategyStatus

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
async def run(tmp_path: Path) -> AsyncIterator[Running]:
    async with running_app(api_config(tmp_path)) as r:
        await wait_for(lambda: r.app.accounts[ACCOUNT].oms.view().last_ticks, what="first tick")
        yield r


def _example_path(path: str) -> str:
    return (
        path.replace("{account_id}", ACCOUNT)
        .replace("{client_order_id}", "nope")
        .replace("{position_id}", "nope")
        .replace("{strategy_id}", "ema")
        .replace("{trade_id}", "nope")
    )


# ====================================================================== contract


def test_committed_openapi_matches_the_code() -> None:
    committed = json.loads((ROOT / "docs" / "api" / "openapi.json").read_text(encoding="utf-8"))
    assert committed == json.loads(json.dumps(openapi_schema())), (
        "docs/api/openapi.json is stale: run `uv run trader openapi --out docs/api/openapi.json`"
    )


async def test_every_route_but_health_needs_the_token(run: Running) -> None:
    paths = openapi_schema()["paths"]
    assert isinstance(paths, dict)
    checked = 0
    async with httpx.AsyncClient(base_url=run.base) as anon:
        for path, ops in paths.items():
            for method in ops:
                url = _example_path(path)
                for headers in ({}, {"Authorization": "Bearer wrong-token"}, {"Authorization": TOKEN}):
                    resp = await anon.request(method, url, headers=headers, json={})
                    if path == "/health":
                        assert resp.status_code == 200
                    else:
                        assert resp.status_code == 401, (method, path, headers, resp.text)
                        assert resp.headers["www-authenticate"] == "Bearer"
                checked += 1
    assert checked >= 25
    # A rejected request is not audited and changed nothing.
    assert (await run.client.get("/audit")).json() == []


async def test_health_is_minimal(run: Running) -> None:
    async with httpx.AsyncClient(base_url=run.base) as anon:
        body = (await anon.get("/health")).json()
    assert body == {"status": "ok", "accounts_connected": 1, "accounts": 1}


async def test_read_endpoints_match_their_models(run: Running) -> None:
    c = run.client
    await wait_for(lambda: run.app.accounts[ACCOUNT].oms.orders(), 20, "strategies trade")
    cases: list[tuple[str, object]] = [
        ("/accounts", list[s.AccountOut]),
        (f"/accounts/{ACCOUNT}/state", s.AccountStateOut),
        ("/positions", list[s.PositionOut]),
        (f"/positions?account={ACCOUNT}&symbol=EURUSD", list[s.PositionOut]),
        ("/orders", list[s.OrderOut]),
        ("/orders?open=true&limit=5", list[s.OrderOut]),
        ("/strategies", list[s.StrategyOut]),
        ("/strategies/ema", s.StrategyOut),
        ("/risk", list[s.RiskAccountOut]),
        (f"/pnl?account={ACCOUNT}", s.PnlOut),
        ("/trades", list[s.TradeOut]),
        ("/analytics/execution", list[s.StatsOut]),
        ("/analytics/execution?by=strategy,order_type", list[s.StatsOut]),
        ("/analytics/performance?by=strategy,symbol", list[s.StatsOut]),
        ("/audit", list[s.AuditOut]),
    ]
    for path, model in cases:
        resp = await c.get(path)
        assert resp.status_code == 200, (path, resp.text)
        TypeAdapter(model).validate_python(resp.json())
    [acct] = (await c.get("/accounts")).json()
    assert acct | {"kill_level": "NONE"} == {
        "id": ACCOUNT,
        "broker": "paper",
        "environment": "demo",
        "connected": True,
        "ready": True,
        "kill_level": "NONE",
    }
    state = (await c.get(f"/accounts/{ACCOUNT}/state")).json()
    assert state["currency"] == "USD"
    assert Decimal(state["balance"]) > 0  # decimal strings, exact
    orders = (await c.get("/orders")).json()
    assert {o["strategy_id"] for o in orders} <= {"ema", "donchian", "rsi"}
    one = orders[0]
    assert (await c.get(f"/orders/{one['client_order_id']}")).json() == one
    assert {st["id"] for st in (await c.get("/strategies")).json()} == {"ema", "donchian", "rsi"}


async def test_errors_are_mapped(run: Running) -> None:
    c = run.client
    checks = [
        ("GET", "/accounts/nope/state", None, 404),
        ("GET", "/orders/nope", None, 404),
        ("GET", "/strategies/nope", None, 404),
        ("GET", "/pnl?account=nope", None, 404),
        ("GET", "/pnl", None, 422),  # account is required
        ("GET", "/analytics/execution?by=bogus", None, 422),
        ("GET", "/orders?limit=0", None, 422),
        ("POST", "/orders", {"account": ACCOUNT, "symbol": "EURUSD", "side": "BUY"}, 422),
        ("POST", "/orders", {"account": ACCOUNT, "symbol": "EURUSD", "side": "BUY", "volume": "-1"}, 422),
        ("POST", "/orders", {"account": "nope", "symbol": "EURUSD", "side": "BUY", "volume": "0.01"}, 404),
        (
            "POST",
            "/orders",  # a LIMIT order needs a limit price: the domain refuses it
            {"account": ACCOUNT, "symbol": "EURUSD", "side": "BUY", "volume": "0.01", "order_type": "LIMIT"},
            422,
        ),
        ("POST", "/orders/nope/cancel", {"reason": "x"}, 404),
        ("POST", "/positions/nope/close", {"account": ACCOUNT}, 404),
        ("POST", "/risk/kill", {"account": ACCOUNT, "level": "NONE", "reason": "x"}, 422),
        ("POST", "/risk/kill", {"account": "nope", "level": "PAUSE_NEW", "reason": "x"}, 404),
        ("POST", "/strategies/nope/pause", {}, 404),
        ("PUT", "/strategies/ema/params", {"params": {"fast": "not a number"}}, 422),
        ("POST", "/trades/nope/notes", {"account": ACCOUNT, "text": "x"}, 404),
        ("POST", "/orders", {"account": ACCOUNT, "unknown": 1}, 422),
    ]
    for method, path, body, status in checks:
        resp = await c.request(method, path, json=body)
        assert resp.status_code == status, (method, path, resp.text)
        assert "detail" in resp.json()
    assert (await c.get("/strategies/ema")).json()["status"] == "RUNNING"  # the bad params changed nothing


# ====================================================================== the one order path


async def test_manual_orders_go_through_risk_and_are_audited(run: Running) -> None:
    c = run.client
    no_sl = {"account": ACCOUNT, "symbol": "EURUSD", "side": "BUY", "volume": "0.01"}
    rejected = (await c.post("/orders", json=no_sl)).json()
    assert rejected["status"] == "REJECTED"
    assert rejected["strategy_id"] == "manual"
    assert rejected["rejection_reason"].startswith("risk:")  # the RiskEngine saw it

    bid, ask = run.price()
    order = (await c.post("/orders", json=no_sl | {"sl": str(bid - Decimal("0.0050")), "reason": "t"})).json()
    assert order["status"] in ("ACCEPTED", "FILLED"), order
    cid = order["client_order_id"]
    oms = run.app.accounts[ACCOUNT].oms
    await wait_for(lambda: any(p.strategy_id == "manual" for p in oms.positions()), what="manual position")
    pos = next(p for p in (await c.get("/positions")).json() if p["strategy_id"] == "manual")
    assert pos["volume"] == "0.01"
    assert (await c.get(f"/orders/{cid}")).json()["status"] == "FILLED"

    closed = await c.post(f"/positions/{pos['position_id']}/close", json={"account": ACCOUNT})
    assert closed.status_code == 200, closed.text
    await wait_for(lambda: not any(p.strategy_id == "manual" for p in oms.positions()), what="closed")
    books = run.app.books[ACCOUNT]
    await wait_for(lambda: any(t.strategy_id == "manual" for t in books.journal.trades()), what="trade")
    [trade] = list((await c.get("/trades?strategy=manual")).json())
    note = await c.post(f"/trades/{trade['trade_id']}/notes", json={"account": ACCOUNT, "text": "by hand"})
    assert note.status_code == 200
    [trade] = (await c.get("/trades?strategy=manual")).json()
    assert trade["notes"] == ["by hand"]
    pnl = (await c.get(f"/pnl?account={ACCOUNT}")).json()
    assert any(line["strategy_id"] == "manual" for line in pnl["by_strategy_symbol"])

    audit = (await c.get("/audit")).json()
    assert [a["action"] for a in audit] == ["trade_note", "position_close", "order_submit", "order_submit"]
    assert {a["actor"] for a in audit} == {"api"}
    assert audit[3]["result"].startswith("REJECTED")
    assert audit[2]["details"]["sl"] == str(bid - Decimal("0.0050"))
    assert (await c.get("/audit?action=order_submit&limit=1")).json() == [audit[2]]
    # The audit file is the durable trail (and audit_log when a database is configured).
    lines = run.app.audit_log.path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    assert (run.app.audit_log.path.stat().st_mode & 0o777) == 0o600
    del ask


async def test_kill_switch_pauses_everything_until_reset(run: Running) -> None:
    c = run.client
    out = (
        await c.post("/risk/kill", json={"account": "*", "level": "PAUSE_NEW", "reason": "api test"})
    ).json()
    assert [r["level"] for r in out] == ["PAUSE_NEW"]
    assert out[0]["triggers"] == {"manual": "PAUSE_NEW: api test"}
    runner = run.app.runner
    assert runner is not None
    await wait_for(
        lambda: all(h.status is StrategyStatus.PAUSED for h in runner.handles.values()), what="paused"
    )
    statuses = {st["id"]: st["status"] for st in (await c.get("/strategies")).json()}
    assert set(statuses.values()) == {"PAUSED"}
    bid, _ = run.price()
    order = {
        "account": ACCOUNT,
        "symbol": "EURUSD",
        "side": "BUY",
        "volume": "0.01",
        "sl": str(bid - Decimal("0.01")),
    }
    blocked = (await c.post("/orders", json=order)).json()
    assert blocked["status"] == "REJECTED"
    assert "kill_switch" in blocked["rejection_reason"]
    resume = await c.post("/strategies/ema/start")
    assert resume.status_code == 409  # cannot resume under a kill switch
    assert run.app.accounts[ACCOUNT].oms.risk_level is KillLevel.PAUSE_NEW

    reset = (await c.post("/risk/reset", json={"account": ACCOUNT, "reason": "done"})).json()
    assert reset["level"] == "NONE"
    assert reset["triggers"] == {}
    await wait_for(
        lambda: all(h.status is StrategyStatus.RUNNING for h in runner.handles.values()), what="resumed"
    )
    actions = [a["action"] for a in (await c.get("/audit")).json()]
    assert actions == ["kill_reset", "strategy_start", "order_submit", "kill"]
    [failed] = (await c.get("/audit?action=strategy_start")).json()
    assert failed["result"].startswith("error:")  # refusals are audited too


async def test_strategy_lifecycle(run: Running) -> None:
    c = run.client
    paused = (await c.post("/strategies/ema/pause", json={"reason": "look"})).json()
    assert (paused["status"], paused["pause_reason"]) == ("PAUSED", "look")
    assert (await c.post("/strategies/ema/pause", json={})).status_code == 409  # not running
    params = (await c.put("/strategies/ema/params", json={"params": {"fast": 3, "slow": 6}})).json()
    assert (params["params"]["fast"], params["params"]["slow"]) == (3, 6)
    assert params["status"] == "PAUSED"  # a paused strategy stays paused with new params
    assert (await c.post("/strategies/ema/start")).json()["status"] == "RUNNING"
    assert (await c.post("/strategies/ema/start")).status_code == 409  # already running
    assert (await c.post("/strategies/ema/stop")).json()["status"] == "STOPPED"
    restarted = (await c.post("/strategies/ema/start")).json()  # a stopped strategy starts fresh
    assert restarted["status"] == "RUNNING"
    assert restarted["params"]["fast"] == 3

    new = {
        "id": "ema2",
        "class": "ema_cross",
        "account": ACCOUNT,
        "symbols": ["EURUSD"],
        "timeframes": ["S1"],
        "params": {"fast": 2, "slow": 5, "atr_period": 2},
    }
    created = await c.post("/strategies", json=new)
    assert created.status_code == 200, created.text
    assert created.json()["status"] == "RUNNING"
    assert (await c.post("/strategies", json=new)).status_code == 422  # duplicate id
    assert (await c.post("/strategies", json=new | {"id": "x1", "account": "nope"})).status_code == 422
    assert (await c.post("/strategies", json=new | {"id": "x2", "symbols": ["GBPUSD"]})).status_code == 422
    assert (await c.post("/strategies", json=new | {"id": "x3", "class": "nope"})).status_code == 422
    # Never an arbitrary import through the API, even of a real strategy class.
    other = new | {"id": "x4", "class": "trader.strategy.examples.ema_cross:EmaCross"}
    assert (await c.post("/strategies", json=other)).status_code == 422
    ids = [st["id"] for st in (await c.get("/strategies")).json()]
    assert ids == ["ema", "donchian", "rsi", "ema2"]
    assert run.app.runner is not None
    assert all(stats.total_errors == 0 for _sid, _st, _r, stats in run.app.runner.status())


async def test_manual_trading_commands(tmp_path: Path) -> None:
    """SPEC §6 manual commands: modify (risk-checked), close all for a symbol or strategy, flatten."""
    async with running_app(api_config(tmp_path, strategies=[])) as r:
        c = r.client
        oms = r.app.accounts[ACCOUNT].oms
        await wait_for(lambda: oms.view().last_ticks, what="ticks")
        bid, _ = r.price()
        limit = bid - Decimal("0.0200")
        pending = (
            await c.post(
                "/orders",
                json={
                    "account": ACCOUNT, "symbol": "EURUSD", "side": "BUY", "volume": "0.01",
                    "order_type": "LIMIT", "limit_price": str(limit), "sl": str(limit - Decimal("0.0050")),
                },
            )
        ).json()  # fmt: skip
        cid = pending["client_order_id"]
        assert pending["status"] in ("ACCEPTED", "WORKING"), pending

        too_big = await c.patch(f"/orders/{cid}", json={"volume": "5"})
        assert too_big.status_code == 409
        assert too_big.json()["detail"].startswith("risk:max_order_volume")
        assert (await c.patch(f"/orders/{cid}", json={})).status_code == 422  # nothing to change
        assert (await c.patch("/orders/nope", json={"volume": "0.02"})).status_code == 404
        ok = await c.patch(f"/orders/{cid}", json={"volume": "0.02"})
        assert ok.status_code == 200, ok.text
        await wait_for(
            lambda: oms.order(cid) is not None and oms.order(cid).request.volume == Decimal("0.02")
        )  # type: ignore[union-attr]

        market = {"account": ACCOUNT, "symbol": "EURUSD", "side": "BUY", "volume": "0.01"}
        bid, _ = r.price()
        await c.post("/orders", json=market | {"sl": str(bid - Decimal("0.0050"))})
        await wait_for(lambda: oms.positions(), what="a position")
        [pos] = oms.positions()
        unprotected = await c.patch(
            f"/positions/{pos.position_id}", json={"account": ACCOUNT, "remove_sl": True}
        )
        assert unprotected.status_code == 409
        assert unprotected.json()["detail"].startswith("risk:loss_per_trade")
        assert pos.sl is not None
        tighter = pos.sl + Decimal("0.0010")
        moved = await c.patch(f"/positions/{pos.position_id}", json={"account": ACCOUNT, "sl": str(tighter)})
        assert moved.status_code == 200, moved.text
        await wait_for(lambda: oms.positions() and oms.positions()[0].sl == tighter, what="the new stop")

        closed = await c.post("/positions/close-all", json={"account": ACCOUNT, "strategy": "nobody"})
        assert closed.json()["message"] == "0 close/cancel requests sent"  # another strategy: nothing
        assert oms.positions()
        flat = await c.post(f"/accounts/{ACCOUNT}/flatten", json={"reason": "end of test"})
        assert flat.json()["message"] == "2 close/cancel requests sent"  # the position and the pending order
        await wait_for(lambda: not oms.positions() and not oms.view().open_orders, what="flat")
        assert oms.risk_level.value == "NONE"  # flatten does not latch the kill switch

        actions = [a["action"] for a in (await c.get("/audit")).json()]
        assert actions[:6] == [
            "flatten",
            "close_all",
            "position_modify",
            "position_modify",
            "order_submit",
            "order_modify",
        ]
        refused = (await c.get("/audit?action=position_modify")).json()[-1]
        assert refused["result"].startswith("error: OrderRejected: risk:loss_per_trade")
