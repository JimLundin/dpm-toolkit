"""Tests for the type registry module."""

import pytest
from sqlalchemy import Boolean, Column, Date, DateTime, Integer, String, Uuid
from sqlalchemy.engine.interfaces import ReflectedColumn

from migrate.type_registry import column_type, enum_type

# ── Helpers ───────────────────────────────────────────────────────────


def _reflected(name: str, type_: object = String()) -> ReflectedColumn:
    """Build a minimal ReflectedColumn dict for testing."""
    return {"name": name, "type": type_}  # type: ignore[typeddict-item]


# ── column_type ───────────────────────────────────────────────────────


class TestColumnTypeExactNames:
    """Exact column name overrides."""

    def test_parent_first(self) -> None:
        result = column_type(_reflected("ParentFirst"))
        assert isinstance(result, Boolean)

    def test_use_interval_arithmetics(self) -> None:
        result = column_type(_reflected("UseIntervalArithmetics"))
        assert isinstance(result, Boolean)

    def test_start_date(self) -> None:
        result = column_type(_reflected("StartDate"))
        assert isinstance(result, DateTime)

    def test_end_date(self) -> None:
        result = column_type(_reflected("EndDate"))
        assert isinstance(result, DateTime)


class TestColumnTypePrefixPatterns:
    """Prefix-based matching: 'is' and 'has'."""

    @pytest.mark.parametrize("name", ["IsActive", "isDeleted", "isEmpty"])
    def test_is_prefix(self, name: str) -> None:
        result = column_type(_reflected(name))
        assert isinstance(result, Boolean)

    @pytest.mark.parametrize("name", ["HasChildren", "hasValue", "HasFlag"])
    def test_has_prefix(self, name: str) -> None:
        result = column_type(_reflected(name))
        assert isinstance(result, Boolean)


class TestColumnTypeSuffixPatterns:
    """Suffix-based matching: 'guid' and 'date'."""

    @pytest.mark.parametrize(
        "name",
        ["ConceptGUID", "RowGUID", "ItemGuid", "someguid"],
    )
    def test_guid_suffix(self, name: str) -> None:
        result = column_type(_reflected(name))
        assert isinstance(result, Uuid)

    @pytest.mark.parametrize(
        "name",
        ["CreationDate", "ModificationDate", "somedate"],
    )
    def test_date_suffix(self, name: str) -> None:
        result = column_type(_reflected(name))
        assert isinstance(result, Date)


class TestColumnTypeNoMatch:
    """Names that should not match any rule."""

    @pytest.mark.parametrize(
        "name",
        ["Name", "Description", "ItemID", "Value", "Count", "ParentItemID"],
    )
    def test_returns_none(self, name: str) -> None:
        assert column_type(_reflected(name)) is None


class TestColumnTypePriorityOrder:
    """Exact-name DateTime rule takes priority over suffix Date rule."""

    def test_start_date_is_datetime_not_date(self) -> None:
        result = column_type(_reflected("StartDate"))
        assert isinstance(result, DateTime)
        assert not isinstance(result, Date) or isinstance(result, DateTime)

    def test_end_date_is_datetime_not_date(self) -> None:
        result = column_type(_reflected("EndDate"))
        assert isinstance(result, DateTime)


# ── enum_type ─────────────────────────────────────────────────────────


class TestEnumType:
    """Tests for enum candidate detection."""

    @pytest.mark.parametrize(
        "name",
        [
            "ItemType",
            "ErrorStatus",
            "OperatorSign",
            "Optionality",
            "FlowDirection",
            "VersionNumber",
            "Endorsement",
            "DataSource",
            "Severity",
            "ErrorCode",
            "OperandReference",
        ],
    )
    def test_string_column_with_enum_suffix_is_enum(self, name: str) -> None:
        col = Column(name, String())
        assert enum_type(col) is True

    @pytest.mark.parametrize(
        "name",
        ["ItemType", "ErrorStatus"],
    )
    def test_non_string_column_is_not_enum(self, name: str) -> None:
        col = Column(name, Integer())
        assert enum_type(col) is False

    @pytest.mark.parametrize(
        "name",
        ["Name", "Description", "ItemID", "Value", "RowGUID"],
    )
    def test_string_column_without_enum_suffix_is_not_enum(self, name: str) -> None:
        col = Column(name, String())
        assert enum_type(col) is False

    def test_case_insensitive_suffix(self) -> None:
        col = Column("itemtype", String())
        assert enum_type(col) is True
