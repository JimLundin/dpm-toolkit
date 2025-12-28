"""Statistics collection from database tables."""

from __future__ import annotations

import re
import uuid
from collections import Counter
from datetime import date, datetime
from typing import Any

from sqlalchemy import Engine, MetaData

from .types import ColumnStatistics


class StatisticsCollector:
    """Collects statistics from a database table."""

    # Maximum number of unique values to track for value_counts
    MAX_VALUE_COUNTS = 100

    # Maximum number of sample values to store
    MAX_SAMPLES = 50

    def __init__(self, engine: Engine) -> None:
        """Initialize with database engine."""
        self.engine = engine
        self.metadata = MetaData()
        self.metadata.reflect(bind=engine)

    def collect_table_statistics(
        self, table_name: str,
    ) -> dict[str, ColumnStatistics]:
        """Collect statistics for all columns in a table."""
        table = self.metadata.tables[table_name]
        column_stats: dict[str, ColumnStatistics] = {}

        # Initialize statistics for each column
        for column in table.columns:
            column_stats[column.name] = ColumnStatistics(
                total_rows=0,
                null_count=0,
                unique_count=0,
            )

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
                row_dict = dict(row._mapping)  # noqa: SLF001
                for column in table.columns:
                    col_name = column.name
                    value = row_dict[col_name]
                    stats = column_stats[col_name]

                    stats.total_rows += 1

                    if value is None:
                        stats.null_count += 1
                    else:
                        # Track unique values
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
            stats.unique_count = len(value_trackers[col_name])
            stats.value_counts = dict(value_counters[col_name])

            # Normalize total_rows (all columns should have same count)
            if stats.total_rows > 0:
                stats.total_rows = stats.total_rows // len(table.columns)

        return column_stats

    def _analyze_value_patterns(
        self, stats: ColumnStatistics, value: Any,  # noqa: ANN401
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
            stats.detected_formats["date"] = (
                stats.detected_formats.get("date", 0) + 1
            )

        # String-based pattern matching
        if isinstance(value, str):
            self._analyze_string_patterns(stats, value)

        # Boolean detection (numeric)
        if isinstance(value, (int, bool)) and value in (0, 1, True, False):
            stats.boolean_pattern_matches += 1

    def _analyze_string_patterns(
        self, stats: ColumnStatistics, value: str,
    ) -> None:
        """Analyze string for UUID, boolean, and date patterns."""
        # UUID pattern detection
        if self._is_uuid_format(value):
            stats.uuid_pattern_matches += 1
            stats.detected_formats["uuid"] = (
                stats.detected_formats.get("uuid", 0) + 1
            )

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
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",  # ISO: 2024-01-15
            r"^\d{2}/\d{2}/\d{4}$",  # DD/MM/YYYY or MM/DD/YYYY
            r"^\d{2}-\d{2}-\d{4}$",  # DD-MM-YYYY
            r"^\d{4}/\d{2}/\d{2}$",  # YYYY/MM/DD
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)

    @staticmethod
    def _is_datetime_format(value: str) -> bool:
        """Check if string matches common datetime formats."""
        datetime_patterns = [
            r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",  # ISO
            r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}",  # DD/MM/YYYY HH:MM:SS
        ]
        return any(re.match(pattern, value) for pattern in datetime_patterns)

    @staticmethod
    def _detect_date_format(value: str) -> str | None:
        """Detect the specific date format of a string."""
        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            return "date_iso"
        if re.match(r"^\d{2}/\d{2}/\d{4}$", value):
            return "date_slash"
        if re.match(r"^\d{2}-\d{2}-\d{4}$", value):
            return "date_dash"
        if re.match(r"^\d{4}/\d{2}/\d{2}$", value):
            return "date_yyyy_slash"
        return None

    @staticmethod
    def _detect_datetime_format(value: str) -> str | None:
        """Detect the specific datetime format of a string."""
        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", value):
            return "datetime_iso_t"
        if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", value):
            return "datetime_iso_space"
        if re.match(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}", value):
            return "datetime_slash"
        return None
