from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trader.domain.ids import is_client_order_id, new_client_order_id
from trader.domain.money import floor_to_step, is_multiple_of, round_to_step, to_decimal
from trader.domain.time import FixedClock, ensure_utc, from_epoch_ms, to_epoch_ms

aware = st.datetimes(
    min_value=datetime(1971, 1, 1),  # noqa: DTZ001 - hypothesis bounds must be naive
    max_value=datetime(2200, 1, 1),  # noqa: DTZ001
    timezones=st.just(UTC),
)
steps = st.sampled_from([Decimal("0.01"), Decimal("0.1"), Decimal(1), Decimal("0.00001"), Decimal("0.25")])
amounts = st.decimals(min_value=0, max_value=10_000, places=6, allow_nan=False, allow_infinity=False)


# ------------------------------------------------------------------ time


@given(st.integers(min_value=0, max_value=7_258_118_400_000))
def test_epoch_ms_round_trip_from_int(ms: int) -> None:
    assert to_epoch_ms(from_epoch_ms(ms)) == ms


@given(aware)
def test_epoch_ms_round_trip_truncates_sub_ms(dt: datetime) -> None:
    back = from_epoch_ms(to_epoch_ms(dt))
    assert back.tzinfo is UTC
    assert timedelta(0) <= dt - back < timedelta(milliseconds=1)


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValueError, match="naive"):
        ensure_utc(datetime(2026, 1, 1))  # noqa: DTZ001
    with pytest.raises(ValueError, match="naive"):
        to_epoch_ms(datetime(2026, 1, 1))  # noqa: DTZ001


def test_ensure_utc_converts_offsets() -> None:
    dt = datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    assert ensure_utc(dt) == datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    assert ensure_utc(dt).tzinfo is UTC


def test_fixed_clock() -> None:
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=UTC))
    clock.advance(timedelta(seconds=5))
    assert clock.now() == datetime(2026, 1, 1, 0, 0, 5, tzinfo=UTC)


# ------------------------------------------------------------------ money


def test_to_decimal_rejects_float_and_non_finite() -> None:
    with pytest.raises(TypeError):
        to_decimal(1.5)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        to_decimal(True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="finite"):
        to_decimal("NaN")
    with pytest.raises(ValueError, match="not a decimal"):
        to_decimal("abc")
    assert to_decimal("1.10") == Decimal("1.10")


@given(amounts, steps)
def test_floor_to_step_properties(value: Decimal, step: Decimal) -> None:
    result = floor_to_step(value, step)
    assert result <= value
    assert value - result < step
    assert is_multiple_of(result, step)


@given(amounts, steps)
def test_round_to_step_is_nearest(value: Decimal, step: Decimal) -> None:
    result = round_to_step(value, step)
    assert is_multiple_of(result, step)
    assert abs(result - value) <= step / 2


def test_step_must_be_positive() -> None:
    with pytest.raises(ValueError, match="step"):
        floor_to_step(Decimal(1), Decimal(0))


# ------------------------------------------------------------------ ids


def test_client_order_ids_are_unique_ulids() -> None:
    ids = {new_client_order_id() for _ in range(1000)}
    assert len(ids) == 1000
    assert all(is_client_order_id(i) and 5 <= len(i) <= 128 for i in ids)
    assert not is_client_order_id("not-a-ulid")
