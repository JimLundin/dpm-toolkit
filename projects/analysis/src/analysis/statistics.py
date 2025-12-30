"""Statistics collection from database tables."""

from __future__ import annotations

import re
import uuid
from collections import Counter
from datetime import date, datetime
from typing import Any

from sqlalchemy import Engine, MetaData

from .types import ColumnStatistics

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

    def __init__(self, engine: Engine) -> None:
        """Initialize with database engine."""
        self.engine = engine
        self.metadata = MetaData()
        self.metadata.reflect(bind=engine)

    def collect_table_statistics(
        self,
        table_name: str,
    ) -> dict[str, ColumnStatistics]:
        """Collect statistics for all columns in a table."""
        table = self.metadata.tables[table_name]

        # Initialize statistics for each column
        column_stats = {
            column.name: ColumnStatistics(
                total_rows=0,
                null_count=0,
                unique_count=0,
            )
            for column in table.columns
        }

        # Collect statistics by reading all rows
        with self.engine.connect() as conn:
            result = conn.execute(table.select())

            value_trackers: dict[str, set[Any]] = {
                col.name: set() for col in table.columns
            }
            value_counters: dict[str, Counter[Any]] = {
                col.name: Counter() for col in table.columns
            }

            for row in result:
                for column in table.columns:
                    col_name = column.name
                    value = row[col_name]
                    stats = column_stats[col_name]

                    stats.total_rows += 1

                    if value is None:
                        stats.null_count += 1
                    else:
                        # Track unique values (with limit to prevent unbounded growth)
                        if len(value_trackers[col_name]) < self.MAX_UNIQUE_TRACKING:
                            value_trackers[col_name].add(value)

                        # Count occurrences for low-cardinality columns
                        if len(value_counters[col_name]) < self.MAX_VALUE_COUNTS:
                            value_counters[col_name][value] += 1

                        # Collect samples
                        if (
                            len(stats.value_samples) < self.MAX_SAMPLES
                            and value not in stats.value_samples
                        ):
                            stats.value_samples.append(value)

                        # Pattern matching
                        self._analyze_value_patterns(stats, value)

        # Finalize statistics
        for col_name, stats in column_stats.items():
            # Note: unique_count may be capped at MAX_UNIQUE_TRACKING
            # If capped, actual count is >= MAX_UNIQUE_TRACKING (acceptable for
            # enum detection since high-cardinality columns aren't candidates)
            stats.unique_count = len(value_trackers[col_name])
            stats.value_counts = dict(value_counters[col_name])

        return column_stats

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
        if isinstance(value, (int, bool)) and value in (0, 1, True, False):
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
        if value.lower() in ("true", "false", "yes", "no", "y", "n", "1", "0"):
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
