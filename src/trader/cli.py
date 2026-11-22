"""Command-line entry point.

    trader run|check-config --config <file> [--confirm-live]
    trader kill --config <file> --account <id> (--level PAUSE_NEW|CANCEL_PENDING|FLATTEN_ALL | --reset)
                --reason <text> [--timeout 60] [--confirm-live]
    trader ctrader-auth [--environment demo] [--secrets-ref CTRADER_DEMO] [--redirect-uri URL]
    trader secrets-key            # create TRADER_SECRETS_KEY in .env (encrypts stored broker tokens)

Secrets are read from the process environment and from ``.env`` (``--env-file``). ``TRADER_MODE`` is the
exception: the live-trading guard only accepts it from the real process environment, never from a file.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path

from trader import __version__
from trader.app import build_account, kill_account, risk_summary
from trader.config.loader import load_config, resolve_mode
from trader.config.secrets import read_env_file, update_env_file
from trader.config.settings import AppConfig
from trader.domain.enums import KillLevel, TradingMode
from trader.domain.errors import BrokerError, ConfigError
from trader.domain.time import SystemClock
from trader.observability.logging import configure_logging, get_logger


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trader", description="Trader platform")
    parser.add_argument("--version", action="version", version=f"trader {__version__}")
    parser.add_argument("--env-file", type=Path, default=Path(".env"), help="secrets file (default .env)")
    sub = parser.add_subparsers(dest="command", required=True)
    auth = sub.add_parser("ctrader-auth", help="authorise a cTrader ID and store OAuth tokens in .env")
    auth.add_argument("--environment", choices=("demo", "live"), default="demo")
    auth.add_argument("--secrets-ref", default=None, help="env prefix (default CTRADER_DEMO / CTRADER_LIVE)")
    auth.add_argument("--redirect-uri", default=None, help="must match a redirect URI of your cTrader app")
    auth.add_argument("--proxy", default=None, help="http://host:port CONNECT proxy, if required")
    sub.add_parser("secrets-key", help="generate TRADER_SECRETS_KEY in .env if it is missing")
    kill = sub.add_parser(
        "kill", help="kill switch for one account: block entries, cancel pending orders, flatten (or reset)"
    )
    kill.add_argument("--config", type=Path, required=True, help="path to YAML config")
    kill.add_argument("--account", required=True, help="account id from the config")
    which = kill.add_mutually_exclusive_group(required=True)
    which.add_argument("--level", choices=[lv.value for lv in KillLevel if lv is not KillLevel.NONE])
    which.add_argument("--reset", action="store_true", help="clear the manual switch and latched breakers")
    kill.add_argument("--reason", required=True, help="recorded with the decision")
    kill.add_argument("--timeout", type=float, default=60.0, help="seconds to wait for cancels/closes")
    kill.add_argument("--confirm-live", action="store_true", help="required for live accounts")
    for name, help_text in (
        ("run", "start the trading process"),
        ("check-config", "validate a config file and the trading-mode safety rules"),
    ):
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("--config", type=Path, required=True, help="path to YAML config")
        cmd.add_argument(
            "--confirm-live",
            action="store_true",
            help="required (together with TRADER_MODE=live) to trade a live account",
        )
    return parser


def live_banner(config: AppConfig) -> str:
    lines = ["", "!" * 72, "!!  LIVE TRADING — REAL MONEY", "!" * 72]
    for acc in config.accounts:
        if acc.environment == "live":
            lines.append(f"!!  account={acc.id} broker={acc.broker} broker_account={acc.account_id}")
            lines.append(f"!!    risk: {risk_summary(acc.risk)}")
    lines += ["!" * 72, ""]
    return "\n".join(lines)


def _kill(args: argparse.Namespace, config: AppConfig, env: Mapping[str, str]) -> int:
    account = next((a for a in config.accounts if a.id == args.account), None)
    if account is None:
        print(
            f"unknown account {args.account!r}; configured: {[a.id for a in config.accounts]}",
            file=sys.stderr,
        )
        return 2
    level = None if args.reset else KillLevel(args.level)
    try:
        services = build_account(config, account, env=env, clock=SystemClock())
        result = asyncio.run(
            kill_account(services, level, args.reason, timeout=timedelta(seconds=args.timeout))
        )
    except (ConfigError, BrokerError) as exc:
        print(f"kill failed: {exc}", file=sys.stderr)
        return 2
    print(
        f"{account.id}: level={result.level} positions_left={result.positions_left} "
        f"pending_left={result.orders_left} {'done' if result.done else 'NOT DONE (timeout)'}"
    )
    return 0 if result.done else 1


def _secrets_key(env_file: Path, env: Mapping[str, str]) -> int:
    from trader.brokers.ctrader.oauth import SECRETS_KEY_ENV, TokenStore

    if env.get(SECRETS_KEY_ENV):
        print(f"{SECRETS_KEY_ENV} already set; unchanged (a new key would make stored tokens unreadable)")
        return 0
    update_env_file(env_file, {SECRETS_KEY_ENV: TokenStore.generate_key()})
    print(f"{SECRETS_KEY_ENV} written to {env_file} (not shown). Back it up somewhere safe.")
    return 0


def _ctrader_auth(args: argparse.Namespace, env: Mapping[str, str]) -> int:
    from trader.brokers.ctrader.auth_cli import run_auth

    ref = args.secrets_ref or ("CTRADER_LIVE" if args.environment == "live" else "CTRADER_DEMO")
    try:
        asyncio.run(
            run_auth(
                env=env,
                env_file=args.env_file,
                secrets_ref=ref,
                environment=args.environment,
                redirect_uri=args.redirect_uri,
                prompt=input,
                out=print,
                proxy=args.proxy,
            )
        )
    except (ConfigError, BrokerError) as exc:
        print(f"ctrader-auth failed: {exc}", file=sys.stderr)
        return 2
    return 0


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    args = _parser().parse_args(argv)
    process_env: Mapping[str, str] = os.environ if env is None else env
    # .env fills in secrets; the process environment wins on conflicts.
    merged = {**read_env_file(args.env_file), **process_env}
    if args.command == "secrets-key":
        return _secrets_key(args.env_file, merged)
    if args.command == "ctrader-auth":
        return _ctrader_auth(args, merged)
    try:
        config = load_config(args.config)
        # TRADER_MODE only counts when it is set in the real process environment, never from .env.
        mode = resolve_mode(config, process_env, confirm_live=args.confirm_live)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    configure_logging(config.logging.level, json=config.logging.json_output)
    log = get_logger("trader.cli")

    if args.command == "check-config":
        print(f"ok: mode={mode} accounts={[a.id for a in config.accounts]}")
        return 0

    if args.command == "kill":
        return _kill(args, config, merged)

    if mode is TradingMode.LIVE:
        print(live_banner(config), file=sys.stderr)
    log.info("startup", mode=str(mode), accounts=[a.id for a in config.accounts], version=__version__)
    # The long-running process (strategies + market data) arrives with the strategy runner (phase 7).
    log.warning("runtime_not_available", detail="no strategy runner yet; exiting after config validation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
