# ADR 0001 — Project scaffold and tooling guards

Status: accepted (phase 0)

## Decision

- Python 3.12 only (`requires-python = "==3.12.*"`), uv-managed, hatchling build, `src/` layout, `trader` console script.
- Runtime dependencies are added **per phase**, only when first used (phase 0: pydantic, pyyaml, python-ulid,
  structlog). The stack in `CLAUDE.md` is unchanged; we just don't install what no code imports yet.
- Rules in `CLAUDE.md` that a tool can check are enforced by tools rather than review:
  - ruff `DTZ` + banned `datetime.utcnow/utcfromtimestamp` → no naive datetimes.
  - ruff `TD003` → no `TODO` without an issue link. `T20` → no `print` outside the CLI.
  - mypy `--strict`; `disallow_any_explicit` for `trader.domain.*` and `trader.oms.*` (no `Any`).
  - `tests/unit/test_architecture.py`: the domain imports only the stdlib (no IO/network modules) and `ulid`;
    no `== "exness"`/`== "ctrader"` comparisons outside `brokers/`; no `float` annotations in domain/OMS.
  - pytest `filterwarnings = error`, `--strict-markers`; network tests use the `demo` marker.
- ruff `N818` (Error suffix) is disabled because the error names are prescribed by `CLAUDE.md`.

## Trading-mode safety (`config/loader.py::resolve_mode`)

- Default mode is `paper`. `TRADER_MODE`, when set, must equal the config `mode` (no silent override).
- `live` requires config `mode: live` **and** `TRADER_MODE=live` **and** `--confirm-live`; a live banner is printed.
- `--confirm-live` outside live mode is an error (it signals confusion about which config is loaded).
- `demo`/`paper` configs may not contain `environment: live` accounts. In `paper` mode the only non-paper
  accounts allowed are those used as a paper account's `market_data_from` source.

## Secrets

Accounts reference secrets with `secrets_ref` (an env-var prefix, e.g. `CTRADER_DEMO` → `CTRADER_DEMO_CLIENT_ID`).
They are loaded as `SecretStr`; missing-secret errors name the variables, never values. YAML forbids unknown keys,
so a pasted `api_key:` fails validation. The structlog redaction processor scrubs sensitive keys, bearer/basic
headers, JWTs and PEM private keys from both structlog and stdlib log records.
