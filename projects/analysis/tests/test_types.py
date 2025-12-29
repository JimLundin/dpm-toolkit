"""Tests for type definitions."""

from analysis.reporting import _dataclass_to_dict
from analysis.types import (
    ColumnStatistics,
    InferredType,
    NamePattern,
    TypeRecommendation,
)


def test_column_statistics_cardinality_ratio() -> None:
    """Test cardinality ratio calculation for enum detection."""
    stats = ColumnStatistics(
        total_rows=100,
        null_count=20,
        unique_count=30,
    )

    # Cardinality ratio is used for enum inference
    assert stats.cardinality_ratio == 30 / 80  # 0.375


def test_column_statistics_all_null_edge_case() -> None:
    """Test that all-null columns have zero cardinality ratio."""
    stats = ColumnStatistics(total_rows=100, null_count=100, unique_count=0)

    # All-null columns should not trigger enum inference
    assert stats.cardinality_ratio == 0.0


def test_type_recommendation_serializes_enum_values() -> None:
    """Test that enum values are properly serialized and sorted."""
    rec = TypeRecommendation(
        table_name="users",
        column_name="status",
        current_type="VARCHAR",
        inferred_type=InferredType.ENUM,
        confidence=0.95,
        evidence={"cardinality": 3},
        enum_values={"pending", "active", "inactive"},  # Unsorted input
    )

    result = _dataclass_to_dict(rec)

    # Enum values should be sorted in output
    assert result["enum_values"] == ["active", "inactive", "pending"]
    # StrEnum should be converted to value
    assert result["inferred_type"] == "enum"
    # Confidence should be rounded to 3 decimals
    assert result["confidence"] == 0.95


def test_name_pattern_dataclass_conversion() -> None:
    """Test that NamePattern dataclass can be converted to dict."""
    pattern = NamePattern(
        pattern_type="suffix",
        pattern="code",
        inferred_type=InferredType.ENUM,
        occurrences=10,
        confidence=0.85,
        examples=[f"col_{i}_code" for i in range(10)],  # 10 examples
    )

    result = _dataclass_to_dict(pattern)

    # All examples should be included (template handles limiting)
    assert len(result["examples"]) == 10
    # StrEnum should be converted to value
    assert result["inferred_type"] == "enum"
