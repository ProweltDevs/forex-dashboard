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
