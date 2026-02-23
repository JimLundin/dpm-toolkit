"""Tests for the value casting system."""

from datetime import UTC, date, datetime
from uuid import UUID

import pytest
from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, Uuid

from migrate.value_casters import (
    cast_boolean,
    cast_date,
    cast_datetime,
    cast_uuid,
    value_caster,
)

# ── cast_boolean ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", True),
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("yes", True),
        ("Yes", True),
        ("y", True),
        ("Y", True),
        ("0", False),
        ("false", False),
        ("no", False),
        ("n", False),
        ("", False),
        ("random", False),
    ],
)
def test_cast_boolean_strings(raw: str, expected: bool) -> None:
    assert cast_boolean(raw) is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (1, True),
        (0, False),
        (True, True),
        (False, False),
        (None, False),
        (3.14, True),
        (0.0, False),
    ],
)
def test_cast_boolean_non_strings(raw: object, expected: bool) -> None:
    assert cast_boolean(raw) is expected  # type: ignore[arg-type]


# ── cast_date ─────────────────────────────────────────────────────────


def test_cast_date_from_date() -> None:
    d = date(2026, 3, 15)
    assert cast_date(d) == d


def test_cast_date_from_datetime() -> None:
    dt = datetime(2026, 3, 15, 10, 30, 0, tzinfo=UTC)
    assert cast_date(dt) == date(2026, 3, 15)


def test_cast_date_iso_format() -> None:
    assert cast_date("2026-03-15") == date(2026, 3, 15)


def test_cast_date_dd_mm_yyyy_slash() -> None:
    assert cast_date("15/03/2026") == date(2026, 3, 15)


def test_cast_date_mm_dd_yyyy_slash() -> None:
    # When day <= 12 this is ambiguous; the DD/MM/YYYY format is tried first
    assert cast_date("30/01/2026") == date(2026, 1, 30)


def test_cast_date_dd_mm_yyyy_dash() -> None:
    assert cast_date("15-03-2026") == date(2026, 3, 15)


def test_cast_date_yyyy_mm_dd_slash() -> None:
    assert cast_date("2026/03/15") == date(2026, 3, 15)


def test_cast_date_invalid_string() -> None:
    with pytest.raises(ValueError, match="Cannot convert"):
        cast_date("not-a-date")


def test_cast_date_invalid_type() -> None:
    with pytest.raises(TypeError, match="Cannot convert int to date"):
        cast_date(42)  # type: ignore[arg-type]


# ── cast_datetime ─────────────────────────────────────────────────────


def test_cast_datetime_from_datetime() -> None:
    dt = datetime(2026, 3, 15, 10, 30, 0, tzinfo=UTC)
    assert cast_datetime(dt) is dt


def test_cast_datetime_from_date() -> None:
    d = date(2026, 3, 15)
    result = cast_datetime(d)
    assert isinstance(result, datetime)
    assert result.date() == d
    assert result.hour == 0
    assert result.minute == 0


def test_cast_datetime_iso_format() -> None:
    result = cast_datetime("2026-03-15T10:30:00")
    assert result.year == 2026
    assert result.month == 3
    assert result.day == 15
    assert result.hour == 10
    assert result.minute == 30


def test_cast_datetime_iso_with_space() -> None:
    result = cast_datetime("2026-03-15 10:30:00")
    assert result.year == 2026
    assert result.hour == 10


def test_cast_datetime_dd_mm_yyyy_hms() -> None:
    result = cast_datetime("15/03/2026 10:30:00")
    assert result.year == 2026
    assert result.month == 3
    assert result.day == 15


def test_cast_datetime_date_only_string() -> None:
    result = cast_datetime("2026-03-15")
    assert result.year == 2026
    assert result.hour == 0


def test_cast_datetime_dd_mm_yyyy_date_only() -> None:
    result = cast_datetime("15/03/2026")
    assert result.year == 2026
    assert result.day == 15


def test_cast_datetime_invalid_string() -> None:
    with pytest.raises(ValueError, match="Cannot convert"):
        cast_datetime("not-a-datetime")


def test_cast_datetime_invalid_type() -> None:
    with pytest.raises(TypeError, match="Cannot convert int to datetime"):
        cast_datetime(42)  # type: ignore[arg-type]


# ── cast_uuid ─────────────────────────────────────────────────────────

_EXAMPLE_UUID = UUID("12345678-1234-5678-1234-567812345678")


def test_cast_uuid_passthrough() -> None:
    assert cast_uuid(_EXAMPLE_UUID) is _EXAMPLE_UUID


def test_cast_uuid_from_string() -> None:
    result = cast_uuid("12345678-1234-5678-1234-567812345678")
    assert result == _EXAMPLE_UUID


def test_cast_uuid_from_string_no_hyphens() -> None:
    result = cast_uuid("12345678123456781234567812345678")
    assert result == _EXAMPLE_UUID


def test_cast_uuid_invalid_string() -> None:
    with pytest.raises(ValueError, match="Cannot convert"):
        cast_uuid("not-a-uuid")


def test_cast_uuid_invalid_type() -> None:
    with pytest.raises(TypeError, match="Cannot convert int to UUID"):
        cast_uuid(42)  # type: ignore[arg-type]


# ── value_caster dispatch ─────────────────────────────────────────────


def test_value_caster_boolean() -> None:
    caster = value_caster(Boolean())
    assert caster is cast_boolean


def test_value_caster_date() -> None:
    caster = value_caster(Date())
    assert caster is cast_date


def test_value_caster_datetime() -> None:
    caster = value_caster(DateTime())
    assert caster is cast_datetime


def test_value_caster_uuid() -> None:
    caster = value_caster(Uuid())
    assert caster is cast_uuid


def test_value_caster_unknown_returns_identity() -> None:
    caster = value_caster(String())
    assert caster("hello") == "hello"


def test_value_caster_integer_returns_identity() -> None:
    caster = value_caster(Integer())
    assert caster(42) == 42


def test_value_caster_float_returns_identity() -> None:
    caster = value_caster(Float())
    assert caster(3.14) == 3.14
