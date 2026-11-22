"""Decimal helpers for money, prices and volumes. Floats never enter the domain."""

from __future__ import annotations

from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Decimal, InvalidOperation

ZERO = Decimal(0)


def to_decimal(value: Decimal | int | str) -> Decimal:
    """Build a finite Decimal from an int, str or Decimal. Floats are rejected on purpose:
    convert them at the adapter boundary with ``Decimal(str(x))`` after checking precision."""
    if isinstance(value, bool) or not isinstance(value, Decimal | int | str):
        raise TypeError(f"expected Decimal, int or str, got {type(value).__name__}")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"not a decimal number: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"decimal must be finite: {value!r}")
    return result


def require_decimal(name: str, value: object) -> None:
    """Runtime guard used in ``__post_init__`` of domain objects built from external data."""
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal, got {type(value).__name__}")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite, got {value}")


def require_positive(name: str, value: Decimal) -> None:
    require_decimal(name, value)
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")


def require_non_negative(name: str, value: Decimal) -> None:
    require_decimal(name, value)
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Largest multiple of ``step`` that is <= ``value`` in magnitude (rounds toward zero)."""
    if step <= 0:
        raise ValueError(f"step must be > 0, got {step}")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def round_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Nearest multiple of ``step`` (banker's rounding on exact ties)."""
    if step <= 0:
        raise ValueError(f"step must be > 0, got {step}")
    return (value / step).to_integral_value(rounding=ROUND_HALF_EVEN) * step


def is_multiple_of(value: Decimal, step: Decimal) -> bool:
    if step <= 0:
        raise ValueError(f"step must be > 0, got {step}")
    return value % step == 0
