"""Strategy class lookup for config: a built-in name or ``package.module:ClassName``."""

from __future__ import annotations

import importlib
from typing import Any

from .base import Strategy

BUILTIN: dict[str, str] = {
    "ema_cross": "trader.strategy.examples.ema_cross:EmaCross",
    "donchian_breakout": "trader.strategy.examples.donchian_breakout:DonchianBreakout",
    "rsi_reversion": "trader.strategy.examples.rsi_reversion:RsiReversion",
}


def resolve(name: str) -> type[Strategy[Any]]:
    path = BUILTIN.get(name, name)
    module_name, sep, attr = path.partition(":")
    if not sep or not module_name or not attr:
        raise ValueError(f"unknown strategy {name!r}: use one of {sorted(BUILTIN)} or 'package.module:Class'")
    cls = getattr(importlib.import_module(module_name), attr, None)
    if not (isinstance(cls, type) and issubclass(cls, Strategy)):
        raise ValueError(f"{path} is not a Strategy subclass")
    return cls
