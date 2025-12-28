"""Tests for type definitions."""

from analysis.types import (
    ColumnStatistics,
    InferredType,
    NamePattern,
    TypeRecommendation,
)


def test_inferred_type_str_enum() -> None:
    """Test that InferredType works as a StrEnum."""
    # StrEnum values are strings
    assert InferredType.ENUM == "enum"
    assert InferredType.BOOLEAN == "boolean"
    assert str(InferredType.DATE) == "date"

    # Can use in string operations
    assert InferredType.ENUM.upper() == "ENUM"
    assert f"Type: {InferredType.UUID}" == "Type: uuid"


def test_column_statistics_properties() -> None:
    """Test ColumnStatistics computed properties."""
    stats = ColumnStatistics(
        total_rows=100,
        null_count=20,
        unique_count=30,
    )

    assert stats.cardinality == 30
    assert stats.non_null_count == 80
    assert stats.null_ratio == 0.2
    assert stats.cardinality_ratio == 30 / 80


def test_column_statistics_edge_cases() -> None:
    """Test ColumnStatistics with edge case values."""
    # All null
    stats = ColumnStatistics(total_rows=100, null_count=100, unique_count=0)
    assert stats.null_ratio == 1.0
    assert stats.cardinality_ratio == 0.0

    # Empty table
    stats = ColumnStatistics(total_rows=0, null_count=0, unique_count=0)
    assert stats.null_ratio == 0.0
    assert stats.cardinality_ratio == 0.0


def test_type_recommendation_to_dict() -> None:
    """Test TypeRecommendation serialization."""
    rec = TypeRecommendation(
        table_name="users",
        column_name="status",
        current_type="VARCHAR",
        inferred_type=InferredType.ENUM,
        confidence=0.95,
        evidence={"cardinality": 3},
        enum_values={"active", "inactive", "pending"},
    )

    result = rec.to_dict()

    assert result["table"] == "users"
    assert result["column"] == "status"
    assert result["inferred_type"] == "enum"
    assert result["confidence"] == 0.95
    assert set(result["enum_values"]) == {"active", "inactive", "pending"}


def test_name_pattern_to_dict() -> None:
    """Test NamePattern serialization."""
    pattern = NamePattern(
        pattern_type="suffix",
        pattern="code",
        inferred_type=InferredType.ENUM,
        occurrences=5,
        confidence=0.85,
        examples=["status_code", "error_code", "country_code"],
    )

    result = pattern.to_dict()

    assert result["pattern_type"] == "suffix"
    assert result["pattern"] == "code"
    assert result["inferred_type"] == "enum"
    assert result["occurrences"] == 5
    assert result["confidence"] == 0.85
    assert len(result["examples"]) == 3
