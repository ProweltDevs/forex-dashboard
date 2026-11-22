"""Command-line entry point.

    trader run --config <file> [--duration 24h] [--confirm-live]
    trader check-config --config <file> [--confirm-live]
    trader kill --config <file> --account <id> (--level PAUSE_NEW|CANCEL_PENDING|FLATTEN_ALL | --reset)
                --reason <text> [--timeout 60] [--confirm-live]
    trader report --config <file> --account <id> [--date YYYY-MM-DD]      # daily report from stored books
    trader pnl-check --config <file> --account <id> [--date YYYY-MM-DD]   # our ledger vs broker statement
    trader ctrader-auth [--environment demo] [--secrets-ref CTRADER_DEMO] [--redirect-uri URL]
    trader secrets-key            # create TRADER_SECRETS_KEY in .env (encrypts stored broker tokens)

Secrets are read from the process environment and from ``.env`` (``--env-file``). ``TRADER_MODE`` is the
exception: the live-trading guard only accepts it from the real process environment, never from a file.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import signal
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from trader import __version__
from trader.analytics.books import offline_report
from trader.app import App, RunReport, build_account, kill_account, risk_summary, run_app, run_pnl_check
from trader.config.loader import load_config, resolve_mode
from trader.config.secrets import read_env_file, update_env_file
from trader.config.settings import AccountConfig, AppConfig
from trader.domain.enums import KillLevel, TradingMode
from trader.domain.errors import BrokerError, ConfigError
from trader.domain.time import SystemClock
from trader.observability.logging import configure_logging, get_logger
from trader.portfolio.ledger import trading_day


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
        ("report", "write a trading day's report (JSON + CSV) from the stored books"),
        ("pnl-check", "compare our fill ledger with the broker's deal statement for a trading day"),
    ):
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("--config", type=Path, required=True, help="path to YAML config")
        cmd.add_argument("--account", required=True, help="account id from the config")
        cmd.add_argument("--date", type=date.fromisoformat, default=None, help="trading day (default: today)")
        cmd.add_argument("--confirm-live", action="store_true", help="required for live accounts")
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
        if name == "run":
            cmd.add_argument("--duration", type=parse_duration, default=None, help="stop after e.g. 90m, 24h")
    return parser


def parse_duration(text: str) -> timedelta:
    m = re.fullmatch(r"(\d+)([smhd])", text.strip())
    if m is None:
        raise argparse.ArgumentTypeError("duration like 30s, 90m, 24h or 2d")
    unit = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}[m.group(2)]
    return timedelta(**{unit: int(m.group(1))})


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


def _report(config: AppConfig, account: AccountConfig, day: date) -> int:
    report, paths = offline_report(config.data_dir, account, day)
    pnl, trades = report["pnl"], report["trades"]
    assert isinstance(pnl, dict)
    assert isinstance(trades, dict)
    realized = pnl["realized"]
    assert isinstance(realized, dict)
    print(
        f"{account.id} {day.isoformat()}: realized net {realized['net']} over {realized['deals']} deals, "
        f"{trades['trades']} trades, win rate {trades['win_rate']}"
    )
    for path in paths:
        print(f"  wrote {path}")
    return 0


def _pnl_check(config: AppConfig, account: AccountConfig, day: date, env: Mapping[str, str]) -> int:
    if account.broker == "paper":
        print(
            "pnl-check needs a broker account (paper deals exist only inside the trading process)",
            file=sys.stderr,
        )
        return 2
    try:
        check = asyncio.run(run_pnl_check(config, account, day, env=env, clock=SystemClock()))
    except (ConfigError, BrokerError) as exc:
        print(f"pnl-check failed: {exc}", file=sys.stderr)
        return 2
    print("\n".join(check.lines()))
    return 0 if check.ok else 1


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
    if args.command in ("report", "pnl-check"):
        account = next((a for a in config.accounts if a.id == args.account), None)
        if account is None:
            print(f"unknown account {args.account!r}", file=sys.stderr)
            return 2
        day = args.date or trading_day(datetime.now(UTC), account.risk.day_offset_minutes)
        if args.command == "report":
            return _report(config, account, day)
        return _pnl_check(config, account, day, merged)

    if mode is TradingMode.LIVE:
        print(live_banner(config), file=sys.stderr)
    log.info("startup", mode=str(mode), accounts=[a.id for a in config.accounts], version=__version__)
    return _run(config, merged, args.duration)


def _run(config: AppConfig, env: Mapping[str, str], duration: timedelta | None) -> int:
    async def main_async() -> RunReport:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        return await run_app(App(config, env=env, clock=SystemClock()), stop, duration)

    try:
        report = asyncio.run(main_async())
    except (ConfigError, BrokerError) as exc:
        print(f"run failed: {exc}", file=sys.stderr)
        return 2
    print("\n".join(report.lines()))
    return 0 if report.strategy_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
