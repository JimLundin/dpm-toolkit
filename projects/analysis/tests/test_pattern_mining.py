"""Tests for pattern mining."""

from analysis.pattern_mining import PatternMiner
from analysis.types import InferredType, TypeRecommendation


def test_mine_suffix_patterns() -> None:
    """Test suffix pattern discovery."""
    miner = PatternMiner()

    recommendations = [
        TypeRecommendation(
            table_name="t1",
            column_name="status_code",
            current_type="VARCHAR",
            inferred_type=InferredType.ENUM,
            confidence=0.9,
        ),
        TypeRecommendation(
            table_name="t1",
            column_name="error_code",
            current_type="VARCHAR",
            inferred_type=InferredType.ENUM,
            confidence=0.85,
        ),
        TypeRecommendation(
            table_name="t2",
            column_name="country_code",
            current_type="VARCHAR",
            inferred_type=InferredType.ENUM,
            confidence=0.88,
        ),
    ]

    patterns = miner.mine_patterns(recommendations)

    # Should find "code" suffix pattern
    code_patterns = [p for p in patterns if p.pattern == "code"]
    assert len(code_patterns) > 0
    assert code_patterns[0].pattern_type == "suffix"
    assert code_patterns[0].inferred_type == InferredType.ENUM
    assert code_patterns[0].occurrences == 3


def test_mine_prefix_patterns() -> None:
    """Test prefix pattern discovery."""
    miner = PatternMiner()

    recommendations = [
        TypeRecommendation(
            table_name="users",
            column_name="is_active",
            current_type="INTEGER",
            inferred_type=InferredType.BOOLEAN,
            confidence=0.95,
        ),
        TypeRecommendation(
            table_name="users",
            column_name="is_admin",
            current_type="INTEGER",
            inferred_type=InferredType.BOOLEAN,
            confidence=0.92,
        ),
        TypeRecommendation(
            table_name="posts",
            column_name="is_published",
            current_type="INTEGER",
            inferred_type=InferredType.BOOLEAN,
            confidence=0.90,
        ),
    ]

    patterns = miner.mine_patterns(recommendations)

    # Should find "is" prefix pattern
    is_patterns = [p for p in patterns if p.pattern == "is"]
    assert len(is_patterns) > 0
    assert is_patterns[0].pattern_type == "prefix"
    assert is_patterns[0].inferred_type == InferredType.BOOLEAN


def test_mine_exact_patterns() -> None:
    """Test exact match pattern discovery."""
    miner = PatternMiner()

    # Same column name across multiple tables
    recommendations = [
        TypeRecommendation(
            table_name="users",
            column_name="Status",
            current_type="VARCHAR",
            inferred_type=InferredType.ENUM,
            confidence=0.90,
        ),
        TypeRecommendation(
            table_name="orders",
            column_name="status",
            current_type="VARCHAR",
            inferred_type=InferredType.ENUM,
            confidence=0.88,
        ),
        TypeRecommendation(
            table_name="payments",
            column_name="STATUS",
            current_type="VARCHAR",
            inferred_type=InferredType.ENUM,
            confidence=0.92,
        ),
    ]

    patterns = miner.mine_patterns(recommendations)

    # Should find "status" pattern (case-insensitive)
    # Can be exact or suffix pattern
    status_patterns = [p for p in patterns if p.pattern == "status"]
    assert len(status_patterns) > 0
    assert status_patterns[0].pattern_type in ("exact", "suffix")
    assert status_patterns[0].occurrences == 3


def test_no_patterns_insufficient_occurrences() -> None:
    """Test that patterns require minimum occurrences."""
    miner = PatternMiner()

    # Only 2 similar columns (less than MIN_OCCURRENCES=3)
    recommendations = [
        TypeRecommendation(
            table_name="t1",
            column_name="user_code",
            current_type="VARCHAR",
            inferred_type=InferredType.ENUM,
            confidence=0.9,
        ),
        TypeRecommendation(
            table_name="t2",
            column_name="product_code",
            current_type="VARCHAR",
            inferred_type=InferredType.ENUM,
            confidence=0.85,
        ),
    ]

    patterns = miner.mine_patterns(recommendations)

    # Should not find "code" pattern with only 2 occurrences
    code_patterns = [p for p in patterns if p.pattern == "code"]
    assert len(code_patterns) == 0
