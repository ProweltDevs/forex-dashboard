"""JSON value types for persisted state (no Any)."""

from __future__ import annotations

type JsonValue = str | int | bool | list[JsonValue] | dict[str, JsonValue] | None
type JsonObj = dict[str, JsonValue]
