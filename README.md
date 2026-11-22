# Trader Platform

Personal algorithmic trading platform for forex/CFDs. Replaces MT5 for order execution, strategy automation,
risk management and P&L analysis. Brokers are plug-and-play adapters (Exness Trader API, cTrader Open API, paper).

- Rules for every change: [`CLAUDE.md`](CLAUDE.md)
- Scope, behaviour and build order: [`docs/SPEC.md`](docs/SPEC.md)
- Design decisions: [`docs/adr/`](docs/adr/)

## Quick start

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run mypy src
uv run pytest -m "not demo"
uv run trader check-config --config config/paper.yaml
```

Default mode is `paper`. Live trading requires `TRADER_MODE=live` **and** `--confirm-live`.

## Secrets

Keys and tokens live in `.env` (gitignored, any `*.env` file is ignored). Start from the template:

```bash
cp .env.example .env && chmod 600 .env
uv run trader secrets-key      # key that encrypts stored broker tokens
```

## Brokers

| Broker | Status | Docs |
|---|---|---|
| paper | contract suite green | [ADR 0003](docs/adr/0003-broker-interface-and-paper-broker.md) |
| cTrader Open API | green on the in-process fake server; demo run pending credentials | [docs/brokers/ctrader.md](docs/brokers/ctrader.md) |
| Exness Trader API | green on the in-process fake server; demo run pending credentials | [docs/brokers/exness.md](docs/brokers/exness.md) |

cTrader demo setup: fill in `CTRADER_DEMO_CLIENT_ID/_CLIENT_SECRET/_REDIRECT_URI` in `.env`, run
`uv run trader ctrader-auth`, set `CTRADER_DEMO_ACCOUNT_ID`, then `uv run pytest -m demo`.

Exness demo setup: fill in the `EXNESS_DEMO_*` block in `.env` (including `EXNESS_DEMO_CONFIRM_DEMO_ACCOUNT=yes`
for a demo account only), then `uv run pytest -m demo`.

## Market data

Ticks feed a non-blocking bus, bar builder and stale-feed monitor, and are recorded to
`data/ticks/{broker}/{symbol}/{yyyy-mm-dd}.parquet` (the future backtest dataset). See
[ADR 0006](docs/adr/0006-market-data.md). `uv run pytest -m demo tests/contract/test_bars_vs_broker.py -s`
compares bars built from live ticks with the broker's candles.

## Orders (OMS)

Every order goes `Strategy → OrderIntent → risk gate → OrderManager → Broker`. One writer task per account
owns order state. PENDING_SUBMIT is persisted before sending, so a crash mid-order is resolved on restart:
the order is found at the broker, or resent once with the same id while the signal is fresh. Reconcile runs
before trading resumes. Missing broker features (expiry, SL/TP, stop-limit, trailing, break-even, time
exits, scale-outs, OCO) are emulated. See [ADR 0007](docs/adr/0007-oms.md).

## Risk

Every new order passes the `RiskEngine`. It applies limits on:
- size and position count;
- exposure and margin;
- loss per trade (an SL is required by default);
- daily loss and drawdown;
- order flow;
- spread, stale prices and fat-finger prices;
- trading sessions, the weekend and blackouts;
- duplicate signals.

Limits are set per account in the `risk:` block, with per-strategy overrides. Every default is listed in
`config/live.yaml.example`. Circuit breakers escalate a kill switch (PAUSE_NEW → CANCEL_PENDING →
FLATTEN_ALL). Its state is persisted, so a restart cannot clear it. See
[ADR 0008](docs/adr/0008-risk-engine.md).

Emergency stop for one broker account (closes and cancels everything on it):

```bash
uv run trader kill --config config/demo.yaml --account ctrader-demo --level FLATTEN_ALL --reason "manual stop"
```

Clear the switch:

```bash
uv run trader kill --config config/demo.yaml --account ctrader-demo --reset --reason "reviewed"
```

Live accounts also need `TRADER_MODE=live` and `--confirm-live`. Acceptance on demo:
`uv run pytest -m demo tests/contract/test_kill_switch_demo.py -s` (the demo account must start flat).
