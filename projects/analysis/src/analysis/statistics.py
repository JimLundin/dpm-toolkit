"""Statistics collection from database tables."""

from __future__ import annotations

import re
import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import MetaData, func, select

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

from .types import (
    BOOLEAN_NUMERIC_VALUES,
    BOOLEAN_STRING_VALUES,
    ColumnStatistics,
)

# Compiled regex patterns for efficient pattern matching
# Single source of truth for all date/datetime format patterns

# Date format patterns: format_name -> compiled_regex
DATE_FORMAT_PATTERNS = {
    "date_iso": re.compile(r"^\d{4}-\d{2}-\d{2}$"),  # ISO: 2024-01-15
    "date_slash": re.compile(r"^\d{2}/\d{2}/\d{4}$"),  # DD/MM/YYYY or MM/DD/YYYY
    "date_dash": re.compile(r"^\d{2}-\d{2}-\d{4}$"),  # DD-MM-YYYY
    "date_yyyy_slash": re.compile(r"^\d{4}/\d{2}/\d{2}$"),  # YYYY/MM/DD
}

# DateTime format patterns: format_name -> compiled_regex
DATETIME_FORMAT_PATTERNS = {
    "datetime_iso_t": re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),  # ISO with T
    "datetime_iso_space": re.compile(
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
    ),  # ISO with space
    "datetime_slash": re.compile(
        r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}",
    ),  # DD/MM/YYYY HH:MM:SS
}


class StatisticsCollector:
    """Collects statistics from a database table."""

    # Maximum number of unique values to track for value_counts
    MAX_VALUE_COUNTS = 100

    # Maximum number of sample values to store
    MAX_SAMPLES = 50

    # Maximum unique values to track per column (prevents unbounded memory growth)
    MAX_UNIQUE_TRACKING = 1000

    # Maximum rows to sample for pattern matching (memory optimization)
    MAX_SAMPLE_ROWS = 10000

    def __init__(self, engine: Engine) -> None:
        """Initialize with database engine."""
        self.engine = engine
        self.metadata = MetaData()
        self.metadata.reflect(bind=engine)

    def collect_table_statistics(
        self,
        table_name: str,
    ) -> dict[str, ColumnStatistics]:
        """Collect statistics for all columns in a table using SQL aggregation."""
        table = self.metadata.tables[table_name]

        with self.engine.connect() as conn:
            # Step 1: Use SQL aggregation for basic statistics
            column_stats = self._collect_basic_statistics(conn, table)

            # Step 2: Use SQL aggregation for value counts (low cardinality columns)
            self._collect_value_counts(conn, table, column_stats)

            # Step 3: Sample rows for pattern matching and samples
            self._collect_patterns_and_samples(conn, table, column_stats)

        return column_stats

    def _collect_basic_statistics(
        self,
        conn: Any,  # noqa: ANN401
        table: Any,  # noqa: ANN401
    ) -> dict[str, ColumnStatistics]:
        """Collect basic statistics using a single SQL query with multiple aggregations.

        This optimization reduces database round-trips from 2N+1 to 1 query.
        For a 50-column table: 101 queries → 1 query.

        """
        # Build single query with all aggregations
        # Total row count + per-column null counts + per-column unique counts
        aggregations = [func.count().label("total_rows")]

        # Add null count for each column: COUNT(*) - COUNT(column)
        # COUNT(*) counts all rows, COUNT(column) counts non-null rows
        # So null_count = COUNT(*) - COUNT(column)
        aggregations.extend(
            (func.count() - func.count(column)).label(f"{column.name}__null")
            for column in table.columns
        )

        # Add unique count for each column using subquery approach
        # (Access doesn't support COUNT(DISTINCT ...) so we use a subquery)
        for column in table.columns:
            # COUNT(*) FROM (SELECT DISTINCT col FROM tbl WHERE col IS NOT NULL)
            distinct_subq = (
                select(column)
                .select_from(table)
                .where(column.isnot(None))
                .distinct()
                .subquery()
            )
            unique_count_subq = (
                select(func.count()).select_from(distinct_subq).scalar_subquery()
            )
            aggregations.append(unique_count_subq.label(f"{column.name}__unique"))

        # Execute single query with all aggregations
        query = select(*aggregations).select_from(table)
        result = conn.execute(query).one()

        # Parse result into per-column statistics
        total_rows = result[0]  # First column is total_rows
        column_stats: dict[str, ColumnStatistics] = {}

        for idx, column in enumerate(table.columns):
            # Null counts start at index 1
            null_count = result[1 + idx]

            # Unique counts start at index 1 + num_columns
            unique_count = result[1 + len(table.columns) + idx]

            column_stats[column.name] = ColumnStatistics(
                total_rows=total_rows or 0,
                null_count=null_count or 0,
                unique_count=min(unique_count or 0, self.MAX_UNIQUE_TRACKING),
            )

        return column_stats

    def _collect_value_counts(
        self,
        conn: Any,  # noqa: ANN401
        table: Any,  # noqa: ANN401
        column_stats: dict[str, ColumnStatistics],
    ) -> None:
        """Collect value counts for low-cardinality columns using SQL."""
        for column in table.columns:
            stats = column_stats[column.name]

            # Only collect value counts for low-cardinality columns
            if stats.unique_count > self.MAX_VALUE_COUNTS:
                continue

            # Use SQL aggregation to get value counts
            query = (
                select(column, func.count().label("count"))
                .select_from(table)
                .where(column.isnot(None))
                .group_by(column)
                .order_by(func.count().desc())
                .limit(self.MAX_VALUE_COUNTS)
            )

            result = conn.execute(query)
            stats.value_counts = {row[0]: row[1] for row in result}

    def _collect_patterns_and_samples(
        self,
        conn: Any,  # noqa: ANN401
        table: Any,  # noqa: ANN401
        column_stats: dict[str, ColumnStatistics],
    ) -> None:
        """Collect pattern matches and samples by iterating over sampled rows."""
        # Sample rows for pattern matching (memory optimization)
        # For large tables, only analyze a sample instead of all rows
        # Order by primary key for deterministic, reproducible sampling
        query = table.select().limit(self.MAX_SAMPLE_ROWS)

        # Add ORDER BY for deterministic sampling
        if table.primary_key.columns:
            # Order by primary key columns for consistency
            query = query.order_by(*table.primary_key.columns)
        else:
            # Fallback: order by first column if no primary key
            query = query.order_by(table.columns[0])

        result = conn.execute(query)

        # Pre-compute column index mapping for efficient tuple access
        # Avoids redundant _asdict() calls in the hot loop
        column_indices = {col.name: idx for idx, col in enumerate(table.columns)}

        # Use sets for O(1) lookup during sample collection
        sample_sets: dict[str, set[Any]] = {col.name: set() for col in table.columns}

        for row in result:
            # Use direct tuple indexing instead of converting to dict
            for column in table.columns:
                col_name = column.name
                value = row[column_indices[col_name]]
                stats = column_stats[col_name]

                if value is None:
                    continue

                # Collect samples using set for O(1) lookup
                if len(sample_sets[col_name]) < self.MAX_SAMPLES:
                    sample_sets[col_name].add(value)

                # Pattern matching
                self._analyze_value_patterns(stats, value)

        # Convert sets to lists for final storage
        for col_name, sample_set in sample_sets.items():
            column_stats[col_name].value_samples = list(sample_set)

    def _analyze_value_patterns(
        self,
        stats: ColumnStatistics,
        value: Any,  # noqa: ANN401
    ) -> None:
        """Analyze value for type pattern matching."""
        # Skip None values
        if value is None:
            return

        # Date/DateTime detection
        if isinstance(value, datetime):
            stats.datetime_pattern_matches += 1
            stats.detected_formats["datetime"] = (
                stats.detected_formats.get("datetime", 0) + 1
            )
        elif isinstance(value, date):
            stats.date_pattern_matches += 1
            stats.detected_formats["date"] = stats.detected_formats.get("date", 0) + 1

        # String-based pattern matching
        if isinstance(value, str):
            self._analyze_string_patterns(stats, value)

        # Boolean detection (numeric)
        if isinstance(value, (int, bool)) and value in BOOLEAN_NUMERIC_VALUES:
            stats.boolean_pattern_matches += 1

    def _analyze_string_patterns(
        self,
        stats: ColumnStatistics,
        value: str,
    ) -> None:
        """Analyze string for UUID, boolean, and date patterns."""
        # UUID pattern detection
        if self._is_uuid_format(value):
            stats.uuid_pattern_matches += 1
            stats.detected_formats["uuid"] = stats.detected_formats.get("uuid", 0) + 1

        # Boolean string detection
        if value.lower() in BOOLEAN_STRING_VALUES:
            stats.boolean_pattern_matches += 1

        # Date pattern detection (various formats)
        if self._is_date_format(value):
            stats.date_pattern_matches += 1
            detected_format = self._detect_date_format(value)
            if detected_format:
                stats.detected_formats[detected_format] = (
                    stats.detected_formats.get(detected_format, 0) + 1
                )

        # DateTime pattern detection
        if self._is_datetime_format(value):
            stats.datetime_pattern_matches += 1
            detected_format = self._detect_datetime_format(value)
            if detected_format:
                stats.detected_formats[detected_format] = (
                    stats.detected_formats.get(detected_format, 0) + 1
                )

    @staticmethod
    def _is_uuid_format(value: str) -> bool:
        """Check if string matches UUID format."""
        try:
            uuid.UUID(value)
        except (ValueError, AttributeError):
            return False
        else:
            return True

    @staticmethod
    def _is_date_format(value: str) -> bool:
        """Check if string matches common date formats."""
        return any(pattern.match(value) for pattern in DATE_FORMAT_PATTERNS.values())

    @staticmethod
    def _is_datetime_format(value: str) -> bool:
        """Check if string matches common datetime formats."""
        return any(
            pattern.match(value) for pattern in DATETIME_FORMAT_PATTERNS.values()
        )

    @staticmethod
    def _detect_date_format(value: str) -> str | None:
        """Detect the specific date format of a string."""
        for format_name, pattern in DATE_FORMAT_PATTERNS.items():
            if pattern.match(value):
                return format_name
        return None

    @staticmethod
    def _detect_datetime_format(value: str) -> str | None:
        """Detect the specific datetime format of a string."""
        for format_name, pattern in DATETIME_FORMAT_PATTERNS.items():
            if pattern.match(value):
                return format_name
        return None
