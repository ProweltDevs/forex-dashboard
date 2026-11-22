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
