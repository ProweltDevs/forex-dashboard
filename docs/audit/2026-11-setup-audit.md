# Setup audit (phase 12), November 2026

**Question asked:** is everything set up correctly?

**Answer:** the setup works from a fresh clone, CI and the container stack are green, and the code keeps
the CLAUDE.md rules, **after** the fixes listed under "Fixed". What still needs a decision or broker
credentials is listed under "Open".

## How it was checked

- **Hands-on:**
  - a fresh clone with `uv sync --locked`;
  - every command in README and CLAUDE.md;
  - every shipped config through `check-config`;
  - a paper `trader run`;
  - the Docker image and compose stack;
  - the CI history;
  - the database migrations against a real PostgreSQL.
- **Two independent read-only reviews:**
  - the code against every CLAUDE.md rule;
  - the code against every SPEC section and phase acceptance criterion, and §17.
- **Every reviewer finding was re-checked against the code before acting.** Some were confirmed by
  reading; for others a test was written that fails without the fix.

## Hands-on results

| Check | Result |
|---|---|
| Fresh clone: `uv sync --locked`, ruff, ruff format, `mypy --strict`, `pytest -m "not demo"` | pass (614 tests before this phase's additions) |
| `trader check-config` on `paper.yaml`, `demo.yaml`, `docker.yaml`, `live.yaml.example` | pass; live is refused without `TRADER_MODE=live` + `--confirm-live`, accepted with both |
| `pytest -m demo` without credentials | all 39 skip, each naming the missing variables |
| Every env var the code reads is in `.env.example` | yes; added a commented `EXNESS_LIVE_*` block |
| Secrets in git (tracked files and history) | none; only placeholders in `.env.example` |
| Local links in the 22 Markdown files | all resolve (two paths are files the runbook tells you to create) |
| `TRADER_DB_URL=… alembic upgrade head`, `alembic check` | pass, no drift; now also an automated test (`test_schema_drift.py`) |
| Paper `trader run` of `config/paper.yaml` from the clean clone | `ready`, 3 strategies, 58 metrics, exit 0, report written |
| Docker: image build, compose stack, healthy and ready, graceful `stop` (exit 0), restart on the volume | pass locally (Docker Hub rate-limited; built from `mirror.gcr.io`) and in CI from Docker Hub |
| CI on `main` | green (check + docker) |
| CI actions | `checkout@v4` and `setup-uv@v5` target the deprecated Node 20: bumped to v7 (Node 24; action.yml and changelogs read), uv pinned like the Dockerfile |

## Fixed

| # | Severity | Finding | Fix | Test |
|---|---|---|---|---|
| 1 | high | **Order and stop changes bypassed risk.** `modify_order` / `modify_position` went straight to the broker: a strategy could raise a pending order's volume past every limit, or remove or widen a stop, even under a kill switch. | `RiskGate.pre_modify` / `pre_modify_position`: changes that add risk go through the kill switch and the pre-trade rules (minus the rate rules), with the order's own exposure replaced, not added. Risk-reducing changes always pass. | `tests/unit/risk/test_modify.py` (incl. a property test), `tests/integration/test_modify_risk.py` |
| 2 | high | **Two writers per account.** `trader kill` while `trader run` managed the account wrote the same OMS snapshot; the running process overwrote the stored kill level and kept trading. | The OMS state file is `flock`ed while an OMS owns it: the second writer is refused with what to use instead. A refused start never writes. The lock dies with its process. | `tests/integration/test_account_lock.py`, chaos `test_trader_kill_is_refused_while_trader_run_manages_the_account` |
| 3 | high | **Exness live gate rested on a label.** No API field tells a demo account from a real one, so a mislabelled `environment: demo` account with real keys would trade in demo mode. | `trader run` / `trader kill` need `<ref>_CONFIRM_DEMO_ACCOUNT=yes` for an Exness demo account, like the demo tests. | `test_registry_builds_exness` |
| 4 | medium | **cTrader `host` override** could point a demo-mode config at the live server. | At every connect the adapter compares the broker's own `isLive` flag with `environment:` and refuses a mismatch (`LiveTradingNotConfirmed`). | `test_refuses_an_account_whose_live_flag_disagrees_with_the_config` |
| 5 | high | **Fills lost from the books at shutdown.** The portfolio stopped before the brokers and cancelled its event consumer without draining it. A fill at shutdown reached the database but not the ledger: the likely cause of the reviewer's 2-in-22 `test_db_kill` failures. | Brokers stop first; the portfolio applies whatever is still queued. | `test_stop_applies_fills_still_queued` (fails without the fix); `test_db_kill` now prints the difference on failure |
| 6 | medium | **Secrets in logged URLs.** cTrader's OAuth call sends `client_secret` and the code as GET parameters; httpx logs URLs, and its exceptions carry them. | Query-parameter redaction in every log record; OAuth errors become domain errors without the exception text. | `test_logging_redaction.py`, `test_ctrader_oauth_errors.py` |
| 7 | medium | **Secrets files not checked** (SPEC §15: 600 permissions). | `.env` and Exness key files that group or others can read are refused, with the `chmod` to run. | `test_secrets_file_must_be_private`, `test_a_dotenv_others_can_read_is_refused` |
| 8 | medium | **Stop-limit fills past the limit unseen.** Exness cannot cap slippage on market-execution instruments; ADR 0005 said the OMS would check fills, but it didn't. | A fill beyond a STOP_LIMIT's limit, or beyond a capped MARKET order's bound, raises `slippage_exceeded` (notified, stored, counted). | `test_fills_past_the_price_protection_raise_an_alert` |
| 9 | medium | **Manual commands missing** (SPEC §6): no modify, close-all by symbol or strategy, or flatten without the kill switch. | `PATCH /orders/{id}`, `PATCH /positions/{id}`, `POST /positions/close-all`, `POST /accounts/{id}/flatten`; risk refusals are 409; every call audited. | `test_manual_trading_commands`, OpenAPI contract |
| 10 | medium | **Synthetic ticks recorded** as if they were market data, into the future backtest dataset. | Synthetic feeds are never recorded. | `test_synthetic_feeds_are_never_recorded` |
| 11 | low | Daily report lacked risk events and execution stats (SPEC §10). | Added. | `test_notifications.py` |
| 12 | low | `FeedEvent`s not persisted (SPEC §12: all domain events except ticks). | Persisted. | — |
| 13 | low | Exness `api_key` in the settings `repr`; CLI tests read a developer's real `.env`. | `repr=False`; every CLI test passes an explicit env file. | `test_exness_settings_repr_hides_the_keys` |
| 14 | low | Documentation drift: wrong ADR path in `state_machine.py`; stale "until phase N" text in ADRs 0003, 0005, 0007, 0011; ADR 0006 breaker claim and tick layout; README alembic command; runbook metric list and live kill command; Telegram reset advice. | Corrected. | — |

## Open: needs credentials or a decision

| Item | Why it is open | Recommendation |
|---|---|---|
| **Demo acceptance** for phases 2, 3, 4, 6, 7 and 8 (contract suite on cTrader and Exness demo, bars vs broker candles, kill-switch flatten, 24 h run, P&L vs statement) and the Telegram probe | Needs your demo credentials: none of these has ever run against a real broker | Fill in `.env`, run `uv run pytest -m demo -s`, record the probe output in `docs/brokers/*.md`, then a 24 h demo `trader run`. **The most important remaining step before live.** |
| cTrader tokens at rest (SPEC §5.2 "encrypted") | `ctrader-auth` writes tokens to `.env` (0600, now enforced); refreshed tokens are encrypted only with `TRADER_SECRETS_KEY`, otherwise kept in memory | Require `TRADER_SECRETS_KEY` for cTrader accounts and keep tokens only in the encrypted store |
| Daily-loss and drawdown breakers use broker-reported equity (SPEC §9 says the live equity "feeds risk") | A design choice in ADR 0010: the broker's number is authoritative, but it arrives every 2–5 s, not every tick | Keep, or feed the portfolio's tick-level equity to risk as an earlier trigger |
| No fill timeout after an ACK on cTrader | A lost execution event leaves an order ACCEPTED until the 5-minute reconcile or a reconnect (Exness has its own watchdog) | A targeted reconcile for market orders still ACCEPTED after ~15 s |
| Exness models hand-written, not generated from the OpenAPI spec (SPEC §5.1) | A deliberate choice in ADR 0005; the spec version is pinned in text only | Vendor the spec file and add a drift test against it |
| Close-by, native trailing stops and OCO are not offered by any adapter (SPEC §5.3 says paper "supports every capability") | Emulated by the OMS instead (trailing, OCO); close-by has no port method | Fine for now; update SPEC or add the capability when a strategy needs it |
| Manual commands in the CLI (SPEC §6 "API/CLI") | Now in the API; the CLI cannot reach a running process | A `trader api ...` client, or use `curl` (runbook) |
| §17 open questions | Answered: Exness signing, the limits format, cTrader price scale and token lifetime. Partial: discovery cache TTL, comment length. Open: cTrader duplicate `clientOrderId`, Exness symbol naming, metal and index contract sizes, commission models | Settle with the demo probes |
| Behaviour implemented but untested | Portfolio drift and balance alerts end to end; ledger backfill after a reconnect; cTrader token-invalidated and client-disconnect events; reconnect jitter; paper latency; the live banner | Add as the code next changes |
| By design, documented | An emulated stop-limit triggers without a new risk check, like a native pending order (PAUSE_NEW keeps pending orders); `ctrader-auth` lives in the CLI; `broker == "paper"` checks in composition code; strategy state saved at most every 200 ms | — |

## Clean (checked, nothing to fix)

- **Hexagonal boundaries:** no broker SDK, protobuf or wire terms outside the adapters.
- **One order path:** API, Telegram and CLI go through `OrderManager`; no other broker calls.
- **Single writer:** the OMS is the only writer of order state; strategies never reach a broker.
- **Strategy determinism:** no wall clock, network, file IO or unseeded randomness in strategies or
  indicators.
- **Reconcile first:** reconcile happens before strategies start and after every reconnect.
- **Types and conventions:**
  - `Decimal` for money in domain, OMS, risk and portfolio;
  - UTC-aware datetimes everywhere;
  - ULID client order ids reused on retries;
  - no `Any` in domain or OMS;
  - adapters raise domain errors.
- **Live gate:** honoured by every entry point (CLI commands, chaos harness, scripts); Docker cannot take
  `TRADER_MODE` from `.env`.
- **Demo tests** are marked and skip without credentials.
- **Secrets handling:**
  - the redaction processor is in place, plus the record factory;
  - Telegram and API tokens are never logged;
  - cTrader token files are encrypted and 0600.
- **Risk defaults:** match `live.yaml.example` field for field; none loosened.
- **No `TODO`s** without an issue reference.
