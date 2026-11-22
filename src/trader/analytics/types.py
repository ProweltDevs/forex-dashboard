"""JSON shapes for analytics output. Unlike the domain's ``JsonValue`` these may hold floats: rates,
latencies and ratios are statistics, not money (money stays Decimal, serialised as strings)."""

from __future__ import annotations

type Out = str | int | float | bool | list[Out] | dict[str, Out] | None
type OutObj = dict[str, Out]
