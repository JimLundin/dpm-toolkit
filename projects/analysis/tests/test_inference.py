"""Tests for type inference engine."""

from analysis.inference import TypeInferenceEngine
from analysis.types import ColumnStatistics, InferredType


def test_infer_enum_low_cardinality() -> None:
    """Test enum detection with low cardinality."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=3,
        value_counts={"active": 500, "inactive": 300, "pending": 200},
    )

    result = engine.infer_type("users", "status", "VARCHAR", stats)

    assert result is not None
    assert result.inferred_type == InferredType.ENUM
    assert result.confidence > 0.7
    assert result.enum_values == {"active", "inactive", "pending"}


def test_infer_enum_too_high_cardinality() -> None:
    """Test that high cardinality columns are not inferred as enums."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=100,  # Too high
    )

    result = engine.infer_type("users", "name", "VARCHAR", stats)

    # Should not be inferred as enum
    assert result is None or result.inferred_type != InferredType.ENUM


def test_infer_boolean_binary_values() -> None:
    """Test boolean detection with binary values."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=2,
        value_counts={"true": 600, "false": 400},
        boolean_pattern_matches=1000,
    )

    result = engine.infer_type("users", "is_active", "VARCHAR", stats)

    assert result is not None
    assert result.inferred_type == InferredType.BOOLEAN
    assert result.confidence >= 0.7


def test_infer_boolean_numeric() -> None:
    """Test boolean detection with 0/1 values."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=2,
        value_counts={0: 300, 1: 700},
        boolean_pattern_matches=1000,
    )

    result = engine.infer_type("users", "active", "INTEGER", stats)

    assert result is not None
    assert result.inferred_type == InferredType.BOOLEAN


def test_infer_boolean_not_boolean_like() -> None:
    """Test that non-boolean binary columns are not inferred as boolean."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=2,
        value_counts={"admin": 100, "user": 900},
    )

    result = engine.infer_type("users", "role", "VARCHAR", stats)

    # Should be enum, not boolean
    assert result is None or result.inferred_type != InferredType.BOOLEAN


def test_infer_uuid() -> None:
    """Test UUID detection."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=1000,
        uuid_pattern_matches=990,  # 99% match
    )

    result = engine.infer_type("users", "id", "VARCHAR", stats)

    assert result is not None
    assert result.inferred_type == InferredType.UUID
    assert result.confidence >= 0.95


def test_no_inference_empty_data() -> None:
    """Test that empty columns return no inference."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=1000,  # All nulls
        unique_count=0,
    )

    result = engine.infer_type("users", "optional_field", "VARCHAR", stats)

    assert result is None


def test_guid_columns_filtered_from_uuid_analysis() -> None:
    """Test that columns with GUID/UUID type are not identified as UUIDs."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=1000,
        uuid_pattern_matches=1000,  # 100% UUID pattern match
    )

    # Test various GUID/UUID type names from different databases:
    # Access/SQL Server use GUID/UNIQUEIDENTIFIER, PostgreSQL uses UUID
    guid_types = [
        "GUID",
        "guid",
        "UNIQUEIDENTIFIER",
        "uniqueidentifier",
        "UUID",
        "uuid",
    ]
    for type_name in guid_types:
        result = engine.infer_type("users", "id", type_name, stats)
        assert result is None, f"Type {type_name} should be filtered from UUID analysis"


def test_non_guid_types_still_identified_as_uuid() -> None:
    """Test that non-GUID types with UUID patterns are still identified."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=1000,
        uuid_pattern_matches=990,  # 99% match
    )

    # Test string types that contain UUID patterns
    result = engine.infer_type("users", "external_id", "VARCHAR", stats)
    assert result is not None
    assert result.inferred_type == InferredType.UUID


def test_integer_columns_not_inferred_as_enum() -> None:
    """Test that integer columns with low cardinality are not inferred as enums."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=3,
        value_counts={1: 500, 2: 300, 3: 200},
    )

    # Test various integer type names
    integer_types = ["INTEGER", "BIGINT", "SMALLINT", "INT", "TINYINT"]
    for type_name in integer_types:
        result = engine.infer_type("users", "status_code", type_name, stats)
        # Should not be inferred as enum (could be boolean if values match pattern)
        assert result is None or result.inferred_type != InferredType.ENUM, (
            f"Type {type_name} should not be inferred as enum"
        )


def test_string_columns_still_inferred_as_enum() -> None:
    """Test that string columns with low cardinality are still inferred as enums."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=3,
        value_counts={"active": 500, "inactive": 300, "pending": 200},
    )

    # Test various string type names
    string_types = ["VARCHAR", "TEXT", "CHAR", "VARCHAR(50)", "NVARCHAR"]
    for type_name in string_types:
        result = engine.infer_type("users", "status", type_name, stats)
        assert result is not None, f"Type {type_name} should be inferred as enum"
        assert result.inferred_type == InferredType.ENUM


def test_boolean_types_filtered_from_boolean_analysis() -> None:
    """Test that columns already typed as boolean are not identified as booleans."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=2,
        value_counts={0: 600, 1: 400},
        boolean_pattern_matches=1000,
    )

    # Test various boolean type names from different databases:
    # BOOLEAN (standard), BOOL (PostgreSQL), BIT (SQL Server), YESNO (Access)
    boolean_types = [
        "BOOLEAN",
        "boolean",
        "BOOL",
        "bool",
        "BIT",
        "bit",
        "YESNO",
        "yesno",
        "BIT(1)",
    ]
    for type_name in boolean_types:
        result = engine.infer_type("users", "is_active", type_name, stats)
        assert result is None, (
            f"Type {type_name} should be filtered from boolean analysis"
        )


def test_non_boolean_types_still_identified_as_boolean() -> None:
    """Test that non-boolean types with boolean patterns are still identified."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=2,
        value_counts={0: 600, 1: 400},
        boolean_pattern_matches=1000,
    )

    # Test types that aren't boolean but contain boolean-like values
    result = engine.infer_type("users", "is_active", "INTEGER", stats)
    assert result is not None
    assert result.inferred_type == InferredType.BOOLEAN


def test_date_types_filtered_from_date_analysis() -> None:
    """Test that columns already typed as date are not identified as dates."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=365,
        date_pattern_matches=1000,
        detected_formats={"date:YYYY-MM-DD": 1000},
    )

    # Test various date type names
    date_types = ["DATE", "date", "Date"]
    for type_name in date_types:
        result = engine.infer_type("users", "birth_date", type_name, stats)
        assert result is None, f"Type {type_name} should be filtered from date analysis"


def test_datetime_types_filtered_from_datetime_analysis() -> None:
    """Test that columns already typed as datetime/timestamp are not identified."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=1000,
        datetime_pattern_matches=1000,
        detected_formats={"datetime:YYYY-MM-DD HH:MM:SS": 1000},
    )

    # Test various datetime/timestamp type names
    datetime_types = [
        "DATETIME",
        "datetime",
        "DateTime",
        "TIMESTAMP",
        "timestamp",
        "DATETIME2",
        "TIMESTAMPTZ",
    ]
    for type_name in datetime_types:
        result = engine.infer_type("users", "created_at", type_name, stats)
        assert result is None, (
            f"Type {type_name} should be filtered from datetime analysis"
        )


def test_string_types_with_date_patterns_still_identified() -> None:
    """Test that string types with date patterns are still identified as dates."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=365,
        date_pattern_matches=990,  # 99% match
        detected_formats={"date:YYYY-MM-DD": 990},
    )

    # Test string types that contain date patterns
    result = engine.infer_type("users", "birth_date", "VARCHAR", stats)
    assert result is not None
    assert result.inferred_type == InferredType.DATE


def test_string_types_with_datetime_patterns_still_identified() -> None:
    """Test that string types with datetime patterns are still identified."""
    engine = TypeInferenceEngine()
    stats = ColumnStatistics(
        total_rows=1000,
        null_count=0,
        unique_count=1000,
        datetime_pattern_matches=990,  # 99% match
        detected_formats={"datetime:YYYY-MM-DD HH:MM:SS": 990},
    )

    # Test string types that contain datetime patterns
    result = engine.infer_type("users", "created_at", "TEXT", stats)
    assert result is not None
    assert result.inferred_type == InferredType.DATETIME
