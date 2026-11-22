"""Command-line entry point: ``trader run|check-config --config <file> [--confirm-live]``."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from trader import __version__
from trader.config.loader import load_config, resolve_mode
from trader.config.settings import AppConfig
from trader.domain.enums import TradingMode
from trader.domain.errors import ConfigError
from trader.observability.logging import configure_logging, get_logger


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="trader", description="Trader platform")
    parser.add_argument("--version", action="version", version=f"trader {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
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
    lines += ["!!  risk limits: see config (risk engine arrives in phase 6)", "!" * 72, ""]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    args = _parser().parse_args(argv)
    env = os.environ if env is None else env
    try:
        config = load_config(args.config)
        mode = resolve_mode(config, env, confirm_live=args.confirm_live)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    configure_logging(config.logging.level, json=config.logging.json_output)
    log = get_logger("trader.cli")

    if args.command == "check-config":
        print(f"ok: mode={mode} accounts={[a.id for a in config.accounts]}")
        return 0

    if mode is TradingMode.LIVE:
        print(live_banner(config), file=sys.stderr)
    log.info("startup", mode=str(mode), accounts=[a.id for a in config.accounts], version=__version__)
    # The composition root (trader.app) is wired up from phase 1 onwards.
    log.warning("runtime_not_available", detail="no broker runtime yet; exiting after config validation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
