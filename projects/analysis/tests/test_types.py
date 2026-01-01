"""Tests for type definitions."""

import json
from dataclasses import asdict
from typing import Any

from analysis.reporting import json_default
from analysis.types import (
    AnalysisReport,
    ColumnStatistics,
    InferredType,
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

    # Use dict_factory to convert sets to sorted lists
    def dict_factory(fields: list[tuple[str, Any]]) -> dict[str, Any]:
        return {k: sorted(v) if isinstance(v, set) and v else v for k, v in fields}

    result = asdict(rec, dict_factory=dict_factory)

    # Enum values should be sorted in output
    assert result["enum_values"] == ["active", "inactive", "pending"]
    # StrEnum remains as StrEnum (it's already a string subclass)
    assert result["inferred_type"] == InferredType.ENUM
    # Confidence is not rounded (formatting is for rendering)
    assert result["confidence"] == 0.95


def test_analysis_report_json_serialization() -> None:
    """Test AnalysisReport serialization to JSON with custom default handler."""
    # Create a complete report
    recommendation = TypeRecommendation(
        table_name="users",
        column_name="status",
        current_type="VARCHAR",
        inferred_type=InferredType.ENUM,
        confidence=0.95,
        evidence={"cardinality": 3},
        enum_values={"pending", "active", "inactive"},  # Set (unsorted)
    )

    report = AnalysisReport(
        database="test.sqlite",
        generated_at="2024-01-01T00:00:00+00:00",
        recommendations=[recommendation],
    )

    # Summary should be computed in __post_init__
    assert report.summary.total_recommendations == 1
    assert report.summary.by_type["enum"] == 1

    # Convert to dict using asdict() (includes summary field)
    report_dict = asdict(report)

    # Serialize to JSON string with custom default handler
    json_str = json.dumps(report_dict, indent=2, default=json_default)

    # Parse back to verify structure
    parsed = json.loads(json_str)

    assert parsed["database"] == "test.sqlite"
    assert parsed["summary"]["total_recommendations"] == 1
    assert parsed["recommendations"][0]["table_name"] == "users"
    # Set should be converted to sorted list
    expected_enum_values = ["active", "inactive", "pending"]
    assert parsed["recommendations"][0]["enum_values"] == expected_enum_values
    # StrEnum should be preserved as string
    assert parsed["recommendations"][0]["inferred_type"] == "enum"
