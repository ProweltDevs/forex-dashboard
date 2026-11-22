# CLAUDE.md — Trader Platform

Personal, production-grade algorithmic trading platform for forex/CFDs. It replaces MT5 for order execution,
strategy automation, risk management and P&L analysis. Brokers are **plug-and-play adapters**; the first two are
**Exness Trader API** and **cTrader Open API**.

Read `docs/SPEC.md` before starting any feature. It is the source of truth for scope, behaviour and build order.
This file holds the rules that apply to every change.

> Out of scope for now: chart UI and backtesting. Design for them (deterministic strategies, a simulator-ready
> broker interface, recorded ticks), but do not build them yet.

---

## Tech stack (decided; ask before changing)

- **Python 3.12**, `asyncio` everywhere (no threads in the core, no blocking IO on the event loop)
- **uv** for dependencies/venv, **ruff** (lint + format), **mypy --strict**, **pytest** + `pytest-asyncio` + `hypothesis`
- **pydantic v2** for config and API schemas; plain frozen `dataclass(slots=True)` for hot-path domain objects
- **httpx** (REST) and **websockets** (WS) for Exness; **protobuf** + raw asyncio TLS sockets for cTrader
- **cryptography** for request signing (Ed25519) where a broker needs it
- **PostgreSQL 16** via SQLAlchemy 2 (async) + asyncpg; **Alembic** migrations
- **pyarrow / Parquet** for the tick recorder
- **FastAPI** for the control API (REST + WebSocket) that the future UI will use
- **structlog** (JSON logs), **prometheus-client** (metrics)
- Docker + docker-compose for deployment

## Commands

```bash
uv sync                                   # install
uv run ruff check . && uv run ruff format --check .
uv run mypy src
uv run pytest -m "not demo"               # unit + integration, no network
uv run pytest -m demo                     # contract tests against broker DEMO accounts (needs creds in .env)
uv run alembic upgrade head
uv run trader run --config config/paper.yaml
```

Run lint, type-check and the non-demo tests before you call any task done.

## Repository layout

```
src/trader/
  domain/        # canonical models, enums, events, errors. NO broker or IO imports.
  brokers/
    base.py      # Broker protocol, Capabilities, BrokerError mapping
    registry.py  # name -> adapter factory ("exness", "ctrader", "paper")
    exness/      # auth.py, discovery.py, rest.py, ws.py, mapping.py, adapter.py
    ctrader/     # proto/ (generated), transport.py, oauth.py, mapping.py, adapter.py
    paper/       # PaperBroker: fills simulated against live ticks
  marketdata/    # tick bus, bar builder, stale-feed monitor, tick recorder
  oms/           # OrderManager, state machine, reconciliation, emulated orders
  risk/          # RiskEngine, rules/, sizing, kill switch, circuit breakers
  strategy/      # Strategy base class, StrategyContext, runner, indicators/, examples/
  portfolio/     # positions, P&L accounting, currency conversion
  analytics/     # trade journal, metrics, execution quality, reports
  persistence/   # db session, repositories, event store, migrations/
  api/           # FastAPI app, routes, ws stream, auth
  notify/        # telegram, email
  config/        # settings models, loaders
  app.py         # composition root (wires everything)
  cli.py
tests/
  unit/  integration/  contract/   # contract/ = the same suite run against every adapter
docs/
  SPEC.md  brokers/exness.md  brokers/ctrader.md  adr/
config/  paper.yaml  demo.yaml  live.yaml.example
```

## Architecture rules (non-negotiable)

1. **Hexagonal / ports & adapters.** Everything above `brokers/` depends only on `brokers/base.py` and `domain/`.
   Broker SDK types, protobuf classes, JSON payloads and broker error codes never leave the adapter package.
2. **One path for orders:** `Strategy -> OrderIntent -> RiskEngine.pre_trade() -> OrderManager -> Broker`.
   Nothing bypasses risk. Manual orders from the API or CLI go through the same path.
3. **The OMS is the only writer of order state.** Each account has a single-writer task. Strategies never call a broker.
4. **Strategies are deterministic and side-effect free.** No wall-clock (`ctx.now()` only), no network, no file IO,
   no randomness without a seeded RNG from the context. They are the same code the future backtester will run.
5. **Broker is the source of truth for money.** Local state is a cache. On startup and after every reconnect:
   reconcile (snapshot positions, orders, account) BEFORE strategies resume.
6. **Capabilities, not if-statements.** Adapters declare `Capabilities` (native trailing stop, partial close,
   hedging, OCO, ...). The OMS emulates missing features. Never write `if broker == "exness"` outside an adapter.
7. **Async-first and non-blocking.** Any CPU-heavy work goes off the event loop.

## Coding conventions

- **Money, prices and volumes are `Decimal`**, never `float`, in domain code. Convert at the adapter boundary
  (cTrader uses int "cents" volumes and 1/100000 price units; see `docs/brokers/ctrader.md`).
- **Volume is lots** in the domain (`Decimal`); adapters convert to broker units using `Instrument.contract_size`.
- **Time:** timezone-aware UTC `datetime` in the domain; int epoch-ms on the wire and in storage. Never naive datetimes.
- **IDs:** the OMS generates a `client_order_id` (ULID) for every order. Adapters pass it as the broker's
  idempotency key or client order ID. Retries reuse the same ID.
- **Symbols:** domain uses canonical names (`EURUSD`, `XAUUSD`). `brokers/<name>/mapping.py` owns the mapping
  to broker symbols and IDs (e.g. Exness suffixes, cTrader numeric `symbolId`).
- Errors: adapters raise `domain.errors` subclasses (`Requote`, `MarketClosed`, `InsufficientMargin`,
  `RateLimited`, `InvalidVolume`, `InvalidPrice`, `AuthExpired`, `BrokerUnavailable`, `UnknownOrderState`).
- Type everything. No `Any` in domain or OMS. Small modules, pure functions where possible.
- Every new behaviour gets tests. Use `hypothesis` for unit conversions, rounding, sizing and P&L maths.

## Safety rules for Claude Code

- **Never place orders on a LIVE account** — not in tests, scripts or "quick checks". Live trading needs
  `TRADER_MODE=live` **and** the CLI flag `--confirm-live`. Default mode is `paper`.
- Network tests are marked `@pytest.mark.demo`, use DEMO credentials only, and are skipped when creds are missing.
- **Never print, log or commit secrets** (API keys, secrets, private keys, OAuth tokens). Use `.env` (gitignored)
  plus `.env.example`. structlog has a redaction processor; keep it.
- Don't loosen risk defaults or remove a risk rule unless explicitly asked.
- **Verify broker API details against the official docs and spec before implementing.** Notes in `docs/SPEC.md` are
  hints, not truth. Record what you confirmed (with links) in `docs/brokers/<name>.md`.
- When something about broker behaviour is unclear, write a demo-marked probe test, run it, and document the result.
  Don't guess.

## Definition of done

Code + tests + types pass, docs updated (`docs/brokers/*.md` or an ADR in `docs/adr/` for design decisions),
no new `TODO` without an issue reference, and the contract suite passes for every adapter you touched.
