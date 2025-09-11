"""Tests for enum detection from SQLite check constraints."""

from schema.enum_detection import (
    detect_enum_for_column,
    values_from_constraint,
    values_from_in_clause,
    values_from_or_clause,
)


def test_simple_in_clause() -> None:
    """Test basic IN clause with string values."""
    constraint = "status IN ('active', 'inactive', 'pending')"
    result = values_from_in_clause(constraint)
    assert result == ["active", "inactive", "pending"]


def test_in_clause_with_spaces() -> None:
    """Test IN clause with various spacing."""
    constraint = "role IN( 'admin' , 'user' , 'moderator' )"
    result = values_from_in_clause(constraint)
    assert result == ["admin", "user", "moderator"]


def test_in_clause_case_insensitive() -> None:
    """Test that IN matching is case insensitive."""
    constraint = "status in ('active', 'inactive')"
    result = values_from_in_clause(constraint)
    assert result == ["active", "inactive"]


def test_in_clause_with_mixed_quotes() -> None:
    """Test IN clause with single quotes."""
    constraint = "type IN ('physical', 'digital', 'service')"
    result = values_from_in_clause(constraint)
    assert result == ["physical", "digital", "service"]


def test_in_clause_single_value() -> None:
    """Test IN clause with single value."""
    constraint = "status IN ('active')"
    result = values_from_in_clause(constraint)
    assert result == ["active"]


def test_in_clause_empty_values() -> None:
    """Test IN clause with no quoted values."""
    constraint = "status IN (1, 2, 3)"
    result = values_from_in_clause(constraint)
    assert result is None


def test_not_in_clause() -> None:
    """Test constraint that's not an IN clause."""
    constraint = "score >= 0 AND score <= 100"
    result = values_from_in_clause(constraint)
    assert result is None


def test_simple_or_clause() -> None:
    """Test basic OR clause with string values."""
    constraint = "category = 'A' OR category = 'B' OR category = 'C'"
    result = values_from_or_clause(constraint)
    assert result == ["A", "B", "C"]


def test_or_clause_with_spaces() -> None:
    """Test OR clause with various spacing."""
    constraint = "size='small' OR size = 'large'"
    result = values_from_or_clause(constraint)
    assert result == ["small", "large"]


def test_or_clause_case_insensitive() -> None:
    """Test that OR matching is case insensitive."""
    constraint = "status = 'active' or status = 'inactive'"
    result = values_from_or_clause(constraint)
    assert result == ["active", "inactive"]


def test_or_clause_single_value() -> None:
    """Test OR clause with single value (should not be detected as enum)."""
    constraint = "status = 'active'"
    result = values_from_or_clause(constraint)
    assert result == ["active"]  # Returns the single value found


def test_not_or_clause() -> None:
    """Test constraint that's not an OR clause."""
    constraint = "score >= 0 AND score <= 100"
    result = values_from_or_clause(constraint)
    assert result == []


def test_in_clause_detected() -> None:
    """Test that IN clause is detected by combined parser."""
    constraint = "status IN ('active', 'inactive', 'pending')"
    result = values_from_constraint(constraint)
    assert result == ["active", "inactive", "pending"]


def test_or_clause_detected() -> None:
    """Test that OR clause is detected by combined parser."""
    constraint = "category = 'A' OR category = 'B' OR category = 'C'"
    result = values_from_constraint(constraint)
    assert result == ["A", "B", "C"]


def test_in_clause_priority() -> None:
    """Test that IN clause takes priority over OR clause."""
    # This is a contrived example but tests the priority
    constraint = "status IN ('active', 'inactive') OR status = 'pending'"
    result = values_from_constraint(constraint)
    # Should detect the IN clause first
    assert result == ["active", "inactive"]


def test_no_enum_detected() -> None:
    """Test constraint with no enum pattern."""
    constraint = "score >= 0 AND score <= 100"
    result = values_from_constraint(constraint)
    assert result is None


def test_single_value_detected() -> None:
    """Test that single values are detected (but may be filtered later)."""
    constraint = "status = 'active'"
    result = values_from_constraint(constraint)
    assert result == ["active"]  # Single value is detected at parse level


def test_column_referenced_in_constraint() -> None:
    """Test enum detection when column is referenced."""
    constraint = "status IN ('active', 'inactive', 'pending')"
    result = detect_enum_for_column(constraint, "status")
    assert result == ["active", "inactive", "pending"]


def test_column_not_referenced() -> None:
    """Test no detection when column is not referenced."""
    constraint = "status IN ('active', 'inactive', 'pending')"
    result = detect_enum_for_column(constraint, "role")
    assert result is None


def test_partial_column_name_match() -> None:
    """Test column name matching behavior."""
    constraint = "user_status IN ('active', 'inactive')"
    # Current implementation uses simple substring matching
    # so 'status' will match 'user_status'
    result = detect_enum_for_column(constraint, "status")
    assert result == ["active", "inactive"]  # Matches due to substring
    # Should definitely match 'user_status' column
    result = detect_enum_for_column(constraint, "user_status")
    assert result == ["active", "inactive"]


def test_or_clause_column_detection() -> None:
    """Test column detection with OR clause."""
    constraint = "category = 'A' OR category = 'B' OR category = 'C'"
    result = detect_enum_for_column(constraint, "category")
    assert result == ["A", "B", "C"]


def test_no_enum_pattern_for_column() -> None:
    """Test column referenced but no enum pattern."""
    constraint = "score >= 0 AND score <= 100"
    result = detect_enum_for_column(constraint, "score")
    assert result is None


def test_empty_constraint() -> None:
    """Test handling of empty constraint text."""
    result = detect_enum_for_column("", "status")
    assert result is None


def test_malformed_in_clause() -> None:
    """Test handling of malformed IN clause."""
    constraint = "status IN (active, inactive"  # Missing closing paren and quotes
    result = detect_enum_for_column(constraint, "status")
    assert result is None


def test_complex_values_in_enum() -> None:
    """Test enum values with special characters."""
    constraint = "status IN ('multi-word', 'with_underscore', 'with space')"
    result = detect_enum_for_column(constraint, "status")
    assert result == ["multi-word", "with_underscore", "with space"]


def test_numeric_strings_in_enum() -> None:
    """Test enum with numeric string values."""
    constraint = "priority IN ('1', '2', '3')"
    result = detect_enum_for_column(constraint, "priority")
    assert result == ["1", "2", "3"]


def test_mixed_case_column_names() -> None:
    """Test column name matching with different cases."""
    constraint = "Status IN ('active', 'inactive')"
    # Should match exact case
    result = detect_enum_for_column(constraint, "Status")
    assert result == ["active", "inactive"]
    # Should not match different case (simple substring matching)
    result = detect_enum_for_column(constraint, "status")
    assert result is None
