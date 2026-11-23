# Runbook

How to deploy, watch and recover the trading process. Design background:
[ADR 0013](adr/0013-ops-hardening.md) (ops), [ADR 0011](adr/0011-persistence.md) (database and WAL),
[ADR 0012](adr/0012-control-api-and-notifications.md) (API and alerts), [ADR 0008](adr/0008-risk-engine.md)
(risk and kill switch).

> **Safety first.** Live trading needs config `mode: live`, `TRADER_MODE=live` in the process
> environment **and** `--confirm-live`. Never test anything on a live account. Paper is the default
> everywhere.

## 1. At a glance

| What | Where |
|---|---|
| Process | `trader run --config <file>`: every account, the strategies, the OMS, risk, persistence, API, ops |
| Ops endpoints (no auth, local only) | `127.0.0.1:9108`: `/health/live`, `/health`, `/metrics` |
| Control API (token) | `127.0.0.1:8700` when `api.enabled`, see [the API guide](api/README.md) |
| State on disk | `data_dir` (`/app/data` in Docker): OMS snapshots `oms/`, books `portfolio/` and `journal/`, strategy state `strategies/`, WAL `wal/db.wal`, audit `audit/audit.jsonl`, reports `reports/`, ticks `ticks/` |
| Database | PostgreSQL 16 (optional): event store and query tables; trading continues without it |
| Secrets | `.env` (chmod 600), never in config files or images |
| Logs | JSON on stderr (`docker compose logs app`); secrets are redacted |

**The broker is the source of truth.** Local state is a cache. Every start and every reconnect
reconciles with the broker before strategies run. That is why restarting the process is always safe.

## 2. Deploy (Docker on a VPS)

1. **Host.** Pick a region close to your broker's trade servers. Compare `send_to_ack` latency (section 4)
   between candidates. Install Docker with the compose plugin.
2. **Time sync.** Install and enable chrony (or systemd-timesyncd):
   ```bash
   sudo apt install chrony && sudo systemctl enable --now chrony && chronyc tracking
   ```
   The process measures its offset against each broker feed (`trader_clock_offset_seconds`) and alerts
   with `clock_drift` beyond `ops.clock_drift_alert_s` (2 s).
3. **Firewall.** Inbound SSH only. The compose file publishes 9108 and 8700 on `127.0.0.1` only; keep
   it that way. For remote API access use a TLS reverse proxy (Caddy, nginx) or an SSH tunnel.
4. **Code and secrets:**
   ```bash
   git clone <repo> trader && cd trader
   cp .env.example .env && chmod 600 .env    # fill in the broker, API and Telegram secrets (0600 is enforced)
   ```
   Change the database password in `docker-compose.yml` (both places).
5. **Config.** Start from `config/docker.yaml` (paper, synthetic prices). For a demo account, copy it to
   `config/demo-docker.yaml`, add the account like `config/demo.yaml`, set `mode: demo`.
6. **Start:**
   ```bash
   docker compose up -d --build
   ```
   ```bash
   TRADER_CONFIG=config/demo-docker.yaml docker compose up -d
   ```
7. **Verify** (section 4): `curl -s 127.0.0.1:9108/health` says `ready`, and the Telegram start notice
   arrives.

**Live** (only after the go-live checklist, section 9). Use an override file, so going live is a
deliberate act, never an edited default:

```yaml
# docker-compose.live.yml
services:
  app:
    command: ["run", "--config", "config/live.yaml", "--confirm-live"]
    environment:
      TRADER_MODE: live
```

```bash
docker compose -f docker-compose.yml -f docker-compose.live.yml up -d
```

`TRADER_MODE` must never come from `.env`. Compose's `env_file` would make `.env` part of the process
environment, so the base `docker-compose.yml` pins `TRADER_MODE` to empty and only this override file
sets it (a test guards both). The process prints a banner with the accounts and risk limits at startup: read it
(`docker compose logs app | head -40`).

**Behind a TLS-intercepting proxy** (corporate networks), pass its CA as a build secret; it never
reaches the image:

```bash
docker build --secret id=build_ca,src=/path/ca.crt --build-arg HTTPS_PROXY=$HTTPS_PROXY -t trader:local .
```

If Docker Hub rate-limits you, `--build-arg PYTHON_IMAGE=mirror.gcr.io/library/python:3.12-slim-bookworm`
uses Google's mirror.

**Without Docker** (development): `uv sync && uv run trader run --config config/paper.yaml`. Run it
under systemd with `Restart=on-failure` and `KillSignal=SIGTERM` / `TimeoutStopSec=60` for the same
behaviour.

## 3. Start, stop, restart, upgrade

| Action | Command | What happens |
|---|---|---|
| Stop | `docker compose stop app` | SIGTERM, then graceful shutdown (below), exit code 0 |
| Start | `docker compose start app` | connect, reconcile, restore state, warm up, start strategies |
| Restart | `docker compose restart app` | both of the above; safe at any time |
| Upgrade | `git pull && docker compose up -d --build app` | stop (graceful), new image, start; DB migrations run on their own |
| Logs | `docker compose logs -f --since 10m app` | JSON lines; `jq` helps |

**Graceful shutdown**, in this order:
1. The API closes.
2. Strategies pause.
3. In-flight orders settle (up to 10 s).
4. Books and reports are written.
5. Brokers disconnect.
6. The database is flushed, or the remainder goes to the WAL.

Orders are never left in `PENDING_SUBMIT` unrecorded. Compose allows 60 s before SIGKILL
(`stop_grace_period`).

**A hard kill** (SIGKILL, OOM, power cut) is also safe. Every order is persisted before it is sent. On the
next start, reconcile resolves each in-flight order:
- **executed while we were down:** adopted as FILLED;
- **never reached the broker:** resent with the same `client_order_id`, once the broker has said twice
  that it has no such order.

The chaos suite proves this (section 10).

**Broker down at startup:** the process stops what it had started and exits with code 2 (`run failed:
cannot connect ...`). Docker (`restart: unless-stopped`) or systemd starts it again, with growing
delays, until the broker answers. A broker lost **while running** never stops the process: it
reconnects by itself and reconciles before trading again.

## 4. Health and monitoring

```bash
curl -s 127.0.0.1:9108/health | jq
```

| Endpoint | 200 when | Used by |
|---|---|---|
| `/health/live` | the process serves requests | Docker `HEALTHCHECK` (restart if hung) |
| `/health` (`/health/ready`) | every account connected and reconciled: `ready`, or `degraded` (kill switch on, stale feed, DB down) | monitoring, alerts |

`/health` answers 503 with `starting`, `not_ready` (lists the accounts) or `stopping`. The body has
per-account connection, readiness, kill level, positions and orders, database status, strategies by
status, and the clock offset per feed.

**Metrics** (`/metrics`, Prometheus format):

| Metric | Watch for |
|---|---|
| `trader_broker_connected`, `trader_account_ready` | 0 for more than a minute |
| `trader_broker_disconnects_total`, `trader_broker_reconnects_total` | a rising rate means a flaky link |
| `trader_kill_level` (0 to 3), `trader_kill_level_changes_total` | above 0: trading is blocked |
| `trader_order_latency_seconds{leg=send_to_ack\|ack_to_fill\|submit_to_fill}` | p95 rising |
| `trader_order_rejects_total{rule}`, `trader_order_retries_total` | bursts |
| `trader_risk_decisions_total{decision,rule}` | rejections by rule |
| `trader_reconcile_corrections_total{kind}` | any (the broker corrected us) |
| `trader_ticks_total`, `trader_last_tick_age_seconds`, `trader_feed_stale` | no ticks while the market is open |
| `trader_clock_offset_seconds` | beyond ±1 s |
| `trader_equity{source=broker\|computed}`, `trader_drawdown`, `trader_open_positions`, `trader_open_orders` | |
| `trader_bus_queue_depth`, `trader_bus_dropped` | a growing queue means a slow consumer |
| `trader_event_loop_lag_seconds` | above 0.1 s: something blocks the loop |
| `trader_db_up`, `trader_db_pending_records` | the database is down, or the WAL is growing |
| `trader_strategy_status`, `trader_strategy_events_total{kind=error}` | |
| `trader_notifications_failed`, `trader_notifications_sent` | alerts are not reaching you |
| `trader_order_status_changes_total{status}`, `trader_fills_total`, `trader_fill_volume_lots_total` | activity (rates) |
| `trader_alerts_total{kind,severity}` | every operator alert, e.g. `slippage_exceeded`, `clock_drift`, `db_down` |
| `trader_build_info{version,mode}`, `trader_start_time_seconds` | which build runs, and restarts |

Example Prometheus alert rules:

```yaml
groups:
  - name: trader
    rules:
      - alert: TraderAccountNotReady
        expr: trader_account_ready == 0
        for: 2m
      - alert: TraderKillSwitch
        expr: trader_kill_level > 0
      - alert: TraderDatabaseDown
        expr: trader_db_up == 0
        for: 5m
      - alert: TraderClockDrift
        expr: abs(trader_clock_offset_seconds) > 1
        for: 5m
      - alert: TraderLoopBlocked
        expr: trader_event_loop_lag_seconds > 0.25
        for: 2m
      - alert: TraderSlowAcks
        expr: histogram_quantile(0.95, sum by (le, account) (rate(trader_order_latency_seconds_bucket{leg="send_to_ack"}[15m]))) > 1
```

## 5. Alerts and what to do

Telegram and email messages start with the mode, e.g. `[live] CRITICAL: Kill switch FLATTEN_ALL`.

| Alert | Means | Do |
|---|---|---|
| **Kill switch** / circuit breaker (critical) | a limit was hit (daily loss, drawdown, disconnect, stale feed) or someone pressed it | Read the reason and the active triggers. Check positions (`/positions`). Fix the cause, then reset (section 6) |
| Risk cleared | the switch is off; strategies resume | nothing |
| **Disconnected** (warning) | the broker connection dropped; reconnecting by itself | Wait about a minute. If it lasts, check the broker status page and the host's network. New orders are refused while down |
| Reconnected | back and reconciled | check for reconcile notices |
| **Reconcile: ...** (warning) | local state differed from the broker; the broker's was applied | Read before/after. `position_adopted`: a position opened elsewhere (manual, another app). `order_resolved`: an in-flight order's outcome was found. Repeated ones need investigating |
| Order rejected | the broker or risk refused an order | the rule is in the message; repeated ones are summarised |
| SL hit / TP hit / position closed | an exit | nothing; the daily report sums them up |
| **db down** (critical) | PostgreSQL unreachable; records buffer to `data/wal/db.wal` | Bring PostgreSQL back (`docker compose ps postgres`, its logs, disk space). The WAL replays by itself; `db recovered` follows |
| **wal write failed** (critical) | the WAL cannot be written (disk full); records are held in memory | Free disk space now (`df -h`; old `ticks/` Parquet files are the usual candidates) |
| **slippage exceeded** (warning) | a STOP_LIMIT filled beyond its limit, or a MARKET order with `max_slippage` beyond that from its signal price (Exness cannot cap slippage on market-execution instruments) | Check the fill (`/orders/{id}`); repeated ones on one instrument: widen the strategy's limits or avoid that order type there |
| **equity drift** / balance mismatch | computed vs broker equity or balance differ | A deposit, withdrawal or fee explains a balance mismatch. Otherwise run `pnl-check` (section 7) |
| **clock drift** | this host's clock is off from the broker's | `chronyc tracking`, `timedatectl`; fix NTP. The offset includes network latency |
| Stale feed | no ticks for a symbol while its session is open | Broker-side quote issue or a lost subscription. The stale breaker may pause entries; it clears when ticks return |
| Strategy error / strategy paused | a callback raised; after `max_errors` the strategy pauses | Read the error in the logs. Fix it, then `POST /strategies/{id}/start` |
| notifications suppressed | the rate limit folded messages | see the logs and `/audit` |
| No alerts at all | Telegram is down or misconfigured: `trader_notifications_failed` rises | check the token and chat id (`docs/notifications.md`) |

## 6. Procedures

### Kill switch

| Situation | How |
|---|---|
| Process running, API on | `curl -X POST -H "Authorization: Bearer $TRADER_API_TOKEN" -H 'content-type: application/json' 127.0.0.1:8700/risk/kill -d '{"account":"*","level":"FLATTEN_ALL","reason":"..."}'` |
| Process running, phone | Telegram `/kill flatten` (or `/kill` to only block new entries) |
| Process **not** running | `docker compose run --rm app kill --config <file> --account <id> --level FLATTEN_ALL --reason "..."`. Live: `docker compose -f docker-compose.yml -f docker-compose.live.yml run --rm app kill ... --confirm-live` (the override sets `TRADER_MODE=live`). It stores the level, so the next start honours it |
| Reset | `POST /risk/reset` with `{"account": "<id>", "reason": "..."}`, or `trader kill --reset` with the process stopped. Never from Telegram |

Levels: `PAUSE_NEW` blocks entries; `CANCEL_PENDING` also cancels pending orders; `FLATTEN_ALL` also
closes every position. `trader kill` while `trader run` manages the same account is refused (the OMS
state file is locked by its owner): use the API or Telegram then.

`POST /accounts/{id}/flatten` closes everything **without** latching the kill switch, and
`POST /positions/close-all` does it for one symbol or strategy (docs/api/README.md).

### After a crash or a hard kill
1. Start the process (Docker does it by itself).
2. Wait for `/health` = `ready`. Reconcile has run.
3. Check `GET /orders?open=true` has no `PENDING_SUBMIT` or `UNKNOWN` orders. A few seconds after start,
   they resolve on their own.
4. Read the reconcile notices.
5. For a live account, run `trader pnl-check` for today (section 7).

### Broker outage
Nothing to do while it reconnects; new orders are refused (503 on the API), never queued. Positions stay
protected by their broker-side SL/TP. Emulated exits (trailing, break-even) need the connection: if
the outage is long and they matter, close positions from the broker's own app. The next reconcile
adopts what you did.

### Database outage
Trading continues. Records go to `data/wal/db.wal` and replay when PostgreSQL is back; this is tested
(`tests/integration/test_db_kill.py`). Do not delete the WAL. A WAL left by a crash replays at the next
start.

### Disk full
Symptoms: the `wal_write_failed` alert, failing log writes. Free space under `data_dir`; old `ticks/`
partitions are safe to archive. Then check that `trader_db_pending_records` drains.

### Rotating secrets
- **`TRADER_API_TOKEN`:** edit `.env`, then `docker compose up -d app` (restart).
- **Telegram:** revoke the token with @BotFather, put the new one in `.env`, restart.
- **cTrader tokens:** refreshed automatically and stored encrypted under `data/secrets/` with
  `TRADER_SECRETS_KEY`. If both are lost, run `trader ctrader-auth` again.
- **Exness keys:** create a new key in Key Management, update `.env`, restart, delete the old key.

## 7. Daily checks

- The daily report notification, or `data/reports/<account>/`.
- **Our ledger against the broker statement:**
  ```bash
  docker compose run --rm app pnl-check --config <file> --account <id> --date <YYYY-MM-DD>
  ```
  Read-only.
- `/health` is `ready`, and Prometheus shows no firing alerts.

## 8. Backups and restore

- **`data_dir`** (the `trader-data` volume) is the process's memory: OMS snapshots, books, strategy state,
  WAL, audit. Back it up daily:
  ```bash
  docker run --rm -v trader_trader-data:/d -v $PWD:/b busybox tar czf /b/trader-data-$(date +%F).tgz -C /d .
  ```
  The volume name is `<project>_trader-data`. A consistent copy needs the app stopped, or accept that
  the broker reconcile fixes the rest.
- **PostgreSQL:** `docker compose exec postgres pg_dump -U trader trader | gzip > trader-$(date +%F).sql.gz`.
- **Restore:** stop the app, restore the volume (and the dump), then start. Reconcile brings the OMS up
  to the broker's state.
- Keep `.env` in a password manager, not with the backups.

## 9. Go-live checklist

- [ ] Weeks on demo with the same strategies and config. Daily `pnl-check` within tolerance.
- [ ] `config/live.yaml` from `config/live.yaml.example`: limits read and set on purpose; `require_sl`
      on; daily loss and drawdown breakers set.
- [ ] Alerts reach your phone (`/status` answers). The kill switch has been tried on demo, from both
      Telegram and the API.
- [ ] Chrony synced; the clock offset is under 0.5 s on demo.
- [ ] Backups scheduled; a restore has been tried.
- [ ] Ports bound to localhost only (`ss -tlnp`); the API, if remote, behind TLS.
- [ ] The live banner read at startup; `docker-compose.live.yml` is the only place that says live.
- [ ] Start with the smallest size.

## 10. Chaos suite

```bash
uv run pytest -m chaos -v
```

Runs in CI with the rest of the tests. Each scenario runs a real `trader run` process against the fake
cTrader server, which outlives it, so a kill is a real kill. After each one, the broker and the trader
must agree: every order executed once, none lost, none left in flight, positions equal.

| Scenario | Proves |
|---|---|
| SIGTERM with an order in flight | graceful exit 0; the order settles and is persisted FILLED; report written |
| SIGKILL with three orders in flight, one lost on the way; restart | executed ones are found and adopted, the lost one is resent once with the same id, no duplicate |
| Connection dropped with an order in flight | reconnect, reconcile finds it (no resend), trading resumes; disconnect/reconnect metrics |
| Broker unreachable, then back | `/health` 503 `not_ready`; orders refused (not queued); recovery when it is back |
| Broker down at startup | exit code 2 after a clean stop; a supervisor restart recovers |
| Notifications down | trading unaffected |
| PostgreSQL killed mid-run (`test_db_kill.py`) | trading continues; the WAL replays; the database equals the books |

`scripts/docker_smoke.sh` (also in CI) checks the image: the compose stack becomes healthy and ready,
and `docker compose stop` exits 0 after a graceful shutdown.
