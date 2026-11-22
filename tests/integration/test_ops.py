"""Ops endpoints on a real App (paper, synthetic feed): /health/live, /health with per-account status
through kill switch and disconnect, and /metrics after real trading."""

from __future__ import annotations

from pathlib import Path

import httpx

from tests.integration.api_helpers import ACCOUNT, api_config, running_app, wait_for
from trader.brokers.paper.adapter import PaperBroker


async def test_health_follows_the_account_and_metrics_count_trading(tmp_path: Path) -> None:
    config = api_config(tmp_path, ops={"enabled": True, "port": 0, "sample_s": 0.2})
    async with running_app(config) as run:
        assert run.app.ops is not None
        ops = httpx.AsyncClient(base_url=f"http://127.0.0.1:{run.app.ops.port}")
        async with ops:
            assert (await ops.get("/health/live")).json() == {"status": "alive"}
            resp = await ops.get("/health")
            body = resp.json()
            assert resp.status_code == 200, body
            assert body["status"] == "ready"
            assert body["mode"] == "paper"
            assert body["accounts"][ACCOUNT] | {"positions": 0, "open_orders": 0} == {
                "broker": "paper",
                "environment": "demo",
                "connected": True,
                "ready": True,
                "kill_level": "NONE",
                "positions": 0,
                "open_orders": 0,
            }
            assert body["database"] == {"configured": False}
            assert body["strategies"] == {"RUNNING": 3}
            assert (await ops.get("/health/ready")).json()["status"] == "ready"

            # A kill switch is not "down", but it needs a look: degraded, still 200.
            await run.client.post(
                "/risk/kill", json={"account": ACCOUNT, "level": "PAUSE_NEW", "reason": "ops"}
            )
            degraded = await ops.get("/health")
            assert degraded.status_code == 200
            assert degraded.json()["status"] == "degraded"
            assert degraded.json()["notes"] == [f"{ACCOUNT}: kill switch PAUSE_NEW"]
            await run.client.post("/risk/reset", json={"account": ACCOUNT, "reason": "ops"})

            # Disconnected: not ready (503) until reconnected and reconciled.
            broker = run.app.accounts[ACCOUNT].broker
            assert isinstance(broker, PaperBroker)
            await broker.simulate_disconnect("ops test")
            await wait_for(lambda: not run.app.accounts[ACCOUNT].oms.ready.is_set(), what="not ready")
            down = await ops.get("/health")
            assert down.status_code == 503
            assert down.json()["status"] == "not_ready"
            assert down.json()["not_ready"] == [ACCOUNT]
            await broker.simulate_reconnect()
            await wait_for(lambda: run.app.accounts[ACCOUNT].oms.ready.is_set(), what="ready again")
            assert (await ops.get("/health")).status_code == 200

            # Metrics after real trading.
            oms = run.app.accounts[ACCOUNT].oms
            await wait_for(lambda: any(o.status.value == "FILLED" for o in oms.orders()), 20, "a fill")
            run.app.monitor.sample()
            text = (await ops.get("/metrics")).text

            def metric(line_start: str) -> float:
                for line in text.splitlines():
                    if line.startswith(line_start):
                        return float(line.rsplit(" ", 1)[1])
                raise AssertionError(f"no {line_start} in /metrics")

            assert metric(f'trader_ticks_total{{feed="{ACCOUNT}",symbol="EURUSD"}}') > 10
            assert metric(f'trader_fills_total{{account="{ACCOUNT}",symbol="EURUSD"}}') >= 1
            assert metric(f'trader_broker_disconnects_total{{account="{ACCOUNT}"}}') == 1
            assert metric(f'trader_broker_reconnects_total{{account="{ACCOUNT}"}}') == 1
            assert metric(f'trader_kill_level_changes_total{{account="{ACCOUNT}",level="PAUSE_NEW"}}') == 1
            assert metric(f'trader_broker_connected{{account="{ACCOUNT}"}}') == 1
            assert metric(f'trader_account_ready{{account="{ACCOUNT}"}}') == 1
            assert metric(f'trader_kill_level{{account="{ACCOUNT}"}}') == 0
            assert metric(f'trader_equity{{account="{ACCOUNT}",source="broker"}}') > 0
            assert metric('trader_strategy_status{status="RUNNING",strategy="ema"}') == 1
            assert metric(f'trader_order_latency_seconds_count{{account="{ACCOUNT}",leg="send_to_ack"}}') >= 1
            assert metric('trader_bus_queue_depth{subscriber="metrics-ticks"}') >= 0
            assert metric("trader_event_loop_lag_seconds") < 1
            assert 'trader_build_info{mode="paper"' in text
    assert run.app.state == "stopped"
