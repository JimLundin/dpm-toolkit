"""Type definitions for the analysis module."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum, auto
from typing import Any

# Boolean value patterns - single source of truth
# Each tuple represents a pair of boolean values (order doesn't matter for our purposes)
# These represent common boolean representations across different database systems
BOOLEAN_VALUE_PAIRS = [
    (0, 1),  # Standard numeric: 0=False, 1=True
    (0, -1),  # Access DB: 0=False, -1=True
    (1, -1),  # Alternative: 1=False, -1=True
    ("true", "false"),  # String boolean
    ("yes", "no"),  # String yes/no
    ("y", "n"),  # String abbreviation
]

# Derive all value sets for comparison (includes both original and string versions)
BOOLEAN_VALUE_SETS = [set(pair) for pair in BOOLEAN_VALUE_PAIRS] + [
    {str(v) for v in pair}
    for pair in BOOLEAN_VALUE_PAIRS
    if not all(isinstance(v, str) for v in pair)  # Add string versions of numeric pairs
]

# Derive flattened string values for efficient pattern matching
BOOLEAN_STRING_VALUES = {str(v).lower() for pair in BOOLEAN_VALUE_PAIRS for v in pair}

# Derive numeric values for detection
BOOLEAN_NUMERIC_VALUES = {
    v
    for pair in BOOLEAN_VALUE_PAIRS
    if all(isinstance(x, int) for x in pair)
    for v in pair
}


class InferredType(StrEnum):
    """Types that can be inferred from data analysis."""

    ENUM = auto()
    BOOLEAN = auto()
    DATE = auto()
    DATETIME = auto()
    UUID = auto()


@dataclass
class ColumnStatistics:
    """Statistics collected for a single column during analysis."""

    # Basic statistics
    total_rows: int
    null_count: int
    unique_count: int

    # Sample values for inspection
    value_samples: list[Any] = field(default_factory=list)

    # Value distribution (for enum detection)
    value_counts: dict[Any, int] = field(default_factory=dict)

    # Pattern matching counters
    date_pattern_matches: int = 0
    datetime_pattern_matches: int = 0
    uuid_pattern_matches: int = 0
    boolean_pattern_matches: int = 0

    # Format detection
    detected_formats: dict[str, int] = field(default_factory=dict)

    @property
    def cardinality(self) -> int:
        """Total number of unique values."""
        return self.unique_count

    @property
    def non_null_count(self) -> int:
        """Number of non-null values."""
        return self.total_rows - self.null_count

    @property
    def null_ratio(self) -> float:
        """Ratio of null values to total rows."""
        if self.total_rows == 0:
            return 0.0
        return self.null_count / self.total_rows

    @property
    def cardinality_ratio(self) -> float:
        """Ratio of unique values to non-null values."""
        if self.non_null_count == 0:
            return 0.0
        return self.unique_count / self.non_null_count


@dataclass
class TypeRecommendation:
    """A recommendation for type refinement."""

    table_name: str
    column_name: str
    current_type: str
    inferred_type: InferredType
    confidence: float  # 0.0 to 1.0
    evidence: dict[str, Any] = field(default_factory=dict)

    # Type-specific data
    enum_values: set[str] | None = None
    detected_format: str | None = None


@dataclass
class NamePattern:
    """A discovered naming pattern correlated with a type."""

    pattern_type: str  # "suffix", "prefix", "exact"
    pattern: str
    inferred_type: InferredType
    occurrences: int
    confidence: float  # 0.0 to 1.0
    examples: list[str] = field(default_factory=list)


@dataclass
class ReportSummary:
    """Summary statistics for an analysis report."""

    total_recommendations: int
    by_type: dict[str, int]
    total_patterns: int
    by_pattern_type: dict[str, int]


@dataclass
class AnalysisReport:
    """Complete analysis report with recommendations and patterns."""

    database: str
    generated_at: str
    recommendations: list[TypeRecommendation]
    patterns: list[NamePattern]
    summary: ReportSummary = field(init=False)

    def __post_init__(self) -> None:
        """Compute summary from recommendations and patterns."""
        by_type = Counter(rec.inferred_type for rec in self.recommendations)
        by_pattern_type = Counter(pat.pattern_type for pat in self.patterns)

        self.summary = ReportSummary(
            total_recommendations=len(self.recommendations),
            by_type={str(k): v for k, v in by_type.items()},
            total_patterns=len(self.patterns),
            by_pattern_type={str(k): v for k, v in by_pattern_type.items()},
        )
