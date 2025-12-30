"""Statistics collection from database tables."""

from __future__ import annotations

import re
import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import Engine, MetaData, func, select

from .types import (
    BOOLEAN_NUMERIC_VALUES,
    BOOLEAN_STRING_VALUES,
    ColumnStatistics,
)

# Compiled regex patterns for efficient pattern matching
_DATE_PATTERNS = [
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),  # ISO: 2024-01-15
    re.compile(r"^\d{2}/\d{2}/\d{4}$"),  # DD/MM/YYYY or MM/DD/YYYY
    re.compile(r"^\d{2}-\d{2}-\d{4}$"),  # DD-MM-YYYY
    re.compile(r"^\d{4}/\d{2}/\d{2}$"),  # YYYY/MM/DD
]

_DATETIME_PATTERNS = [
    re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"),  # ISO
    re.compile(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}"),  # DD/MM/YYYY HH:MM:SS
]

# Specific format detection patterns
_DATE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATE_SLASH = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_DATE_DASH = re.compile(r"^\d{2}-\d{2}-\d{4}$")
_DATE_YYYY_SLASH = re.compile(r"^\d{4}/\d{2}/\d{2}$")

_DATETIME_ISO_T = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
_DATETIME_ISO_SPACE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
_DATETIME_SLASH = re.compile(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}")


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
        """Collect basic statistics using SQL aggregation."""
        # Get total row count once for the table
        total_rows = conn.execute(select(func.count()).select_from(table)).scalar()

        column_stats = {}
        for column in table.columns:
            # Get null count using SQL
            null_count = conn.execute(
                select(func.count()).where(column.is_(None)).select_from(table),
            ).scalar()

            # Get unique count using SQL (capped at MAX_UNIQUE_TRACKING for memory)
            # For high-cardinality columns, this gives us an exact count without
            # loading all values into memory
            unique_count = conn.execute(
                select(func.count(func.distinct(column))).select_from(table),
            ).scalar()

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
        query = table.select().limit(self.MAX_SAMPLE_ROWS)
        result = conn.execute(query)

        for row in result:
            # Convert row to dictionary using named tuple method
            row_dict = row._asdict()

            for column in table.columns:
                col_name = column.name
                value = row_dict[col_name]
                stats = column_stats[col_name]

                if value is None:
                    continue

                # Collect samples
                if (
                    len(stats.value_samples) < self.MAX_SAMPLES
                    and value not in stats.value_samples
                ):
                    stats.value_samples.append(value)

                # Pattern matching
                self._analyze_value_patterns(stats, value)

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
        return any(pattern.match(value) for pattern in _DATE_PATTERNS)

    @staticmethod
    def _is_datetime_format(value: str) -> bool:
        """Check if string matches common datetime formats."""
        return any(pattern.match(value) for pattern in _DATETIME_PATTERNS)

    @staticmethod
    def _detect_date_format(value: str) -> str | None:
        """Detect the specific date format of a string."""
        if _DATE_ISO.match(value):
            return "date_iso"
        if _DATE_SLASH.match(value):
            return "date_slash"
        if _DATE_DASH.match(value):
            return "date_dash"
        if _DATE_YYYY_SLASH.match(value):
            return "date_yyyy_slash"
        return None

    @staticmethod
    def _detect_datetime_format(value: str) -> str | None:
        """Detect the specific datetime format of a string."""
        if _DATETIME_ISO_T.match(value):
            return "datetime_iso_t"
        if _DATETIME_ISO_SPACE.match(value):
            return "datetime_iso_space"
        if _DATETIME_SLASH.match(value):
            return "datetime_slash"
        return None
