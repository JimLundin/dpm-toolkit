"""Tests for enum detection from SQLite check constraints."""

from schema.enum_detection import detect_enum_for_column


def test_simple_in_clause() -> None:
    """Test basic IN clause with string values."""
    constraint = "status IN ('active', 'inactive', 'pending')"
    result = detect_enum_for_column(constraint, "status")
    assert result == ["active", "inactive", "pending"]


def test_in_clause_with_spaces() -> None:
    """Test IN clause with various spacing."""
    constraint = "role IN( 'admin' , 'user' , 'moderator' )"
    result = detect_enum_for_column(constraint, "role")
    assert result == ["admin", "user", "moderator"]


def test_in_clause_case_insensitive() -> None:
    """Test that IN matching is case insensitive."""
    constraint = "status in ('active', 'inactive')"
    result = detect_enum_for_column(constraint, "status")
    assert result == ["active", "inactive"]


def test_in_clause_with_mixed_quotes() -> None:
    """Test IN clause with single quotes."""
    constraint = "type IN ('physical', 'digital', 'service')"
    result = detect_enum_for_column(constraint, "type")
    assert result == ["physical", "digital", "service"]


def test_in_clause_single_value() -> None:
    """Test IN clause with single value."""
    constraint = "status IN ('active')"
    result = detect_enum_for_column(constraint, "status")
    assert result == ["active"]


def test_in_clause_empty_values() -> None:
    """Test IN clause with no quoted values."""
    constraint = "status IN (1, 2, 3)"
    result = detect_enum_for_column(constraint, "status")
    assert result == []


def test_not_in_clause() -> None:
    """Test constraint that's not an IN clause."""
    constraint = "score >= 0 AND score <= 100"
    result = detect_enum_for_column(constraint, "score")
    assert result == []


def test_in_clause_detected() -> None:
    """Test that IN clause is detected."""
    constraint = "status IN ('active', 'inactive', 'pending')"
    result = detect_enum_for_column(constraint, "status")
    assert result == ["active", "inactive", "pending"]


def test_no_enum_detected() -> None:
    """Test constraint with no enum pattern."""
    constraint = "score >= 0 AND score <= 100"
    result = detect_enum_for_column(constraint, "score")
    assert result == []


def test_column_referenced_in_constraint() -> None:
    """Test enum detection when column is referenced."""
    constraint = "status IN ('active', 'inactive', 'pending')"
    result = detect_enum_for_column(constraint, "status")
    assert result == ["active", "inactive", "pending"]


def test_column_not_referenced() -> None:
    """Test no detection when column is not referenced."""
    constraint = "status IN ('active', 'inactive', 'pending')"
    result = detect_enum_for_column(constraint, "role")
    assert result == []


def test_partial_column_name_match() -> None:
    """Test column name matching behavior."""
    constraint = "user_status IN ('active', 'inactive')"
    # Fixed implementation uses exact column name matching
    # so 'status' will NOT match 'user_status'
    result = detect_enum_for_column(constraint, "status")
    assert result == []  # No match - exact column name matching
    # Should definitely match 'user_status' column
    result = detect_enum_for_column(constraint, "user_status")
    assert result == ["active", "inactive"]


def test_no_enum_pattern_for_column() -> None:
    """Test column referenced but no enum pattern."""
    constraint = "score >= 0 AND score <= 100"
    result = detect_enum_for_column(constraint, "score")
    assert result == []


def test_empty_constraint() -> None:
    """Test handling of empty constraint text."""
    result = detect_enum_for_column("", "status")
    assert result == []


def test_malformed_in_clause() -> None:
    """Test handling of malformed IN clause."""
    constraint = "status IN (active, inactive"  # Missing closing paren and quotes
    result = detect_enum_for_column(constraint, "status")
    assert result == []


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
    assert result == []