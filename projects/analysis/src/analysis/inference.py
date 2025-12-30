"""Type inference engine for recommending type refinements."""

from __future__ import annotations

from collections.abc import Iterable

from .types import (
    BOOLEAN_VALUE_SETS,
    ColumnStatistics,
    InferredType,
    TypeRecommendation,
)


class TypeInferenceEngine:
    """Infers better types from collected statistics."""

    # Tunable thresholds
    ENUM_CARDINALITY_THRESHOLD = 50  # Max unique values for enum
    ENUM_CARDINALITY_RATIO = 0.01  # Max cardinality/total_rows ratio
    PATTERN_MATCH_THRESHOLD = 0.95  # Min ratio of pattern matches
    HIGH_CONFIDENCE = 0.9
    MEDIUM_CONFIDENCE = 0.7
    MIN_DISTINCT_VALUES = 2  # Minimum distinct values for enum
    BOOLEAN_VALUE_COUNT = 2  # Boolean columns have exactly 2 values

    # Enum confidence calculation parameters
    ENUM_DATA_THRESHOLD = 1000  # Row count threshold for data boost
    ENUM_DATA_BOOST = 0.2  # Confidence boost for large datasets
    ENUM_RATIO_PENALTY = 0.3  # Penalty multiplier for high cardinality ratio

    def infer_type(  # noqa: PLR0911
        self,
        table_name: str,
        column_name: str,
        current_type: str,
        stats: ColumnStatistics,
    ) -> TypeRecommendation | None:
        """Infer better type from column statistics."""
        # Skip if no data
        if stats.non_null_count == 0:
            return None

        # Try different type inference rules in priority order

        # 1. UUID detection (high precision)
        uuid_rec = self._infer_uuid(table_name, column_name, current_type, stats)
        if uuid_rec and uuid_rec.confidence >= self.HIGH_CONFIDENCE:
            return uuid_rec

        # 2. Boolean detection
        bool_rec = self._infer_boolean(
            table_name,
            column_name,
            current_type,
            stats,
        )
        if bool_rec and bool_rec.confidence >= self.MEDIUM_CONFIDENCE:
            return bool_rec

        # 3. Date/DateTime detection
        datetime_rec = self._infer_datetime(
            table_name,
            column_name,
            current_type,
            stats,
        )
        if datetime_rec and datetime_rec.confidence >= self.HIGH_CONFIDENCE:
            return datetime_rec

        date_rec = self._infer_date(table_name, column_name, current_type, stats)
        if date_rec and date_rec.confidence >= self.HIGH_CONFIDENCE:
            return date_rec

        # 4. Enum detection (low cardinality)
        enum_rec = self._infer_enum(table_name, column_name, current_type, stats)
        if enum_rec and enum_rec.confidence >= self.MEDIUM_CONFIDENCE:
            return enum_rec

        return None

    def _get_most_common_format(
        self,
        formats: dict[str, int],
        filter_key: str | None = None,
    ) -> str | None:
        """Find the most common format from detected formats.

        Args:
            formats: Dictionary mapping format names to occurrence counts
            filter_key: Optional substring that format names must contain

        Returns:
            The most common format name, or None if no formats found

        """
        if filter_key:
            items: Iterable[tuple[str, int]] = (
                (k, v) for k, v in formats.items() if filter_key in k
            )
        else:
            items = formats.items()

        return max(items, key=lambda x: x[1], default=(None, 0))[0]

    def _infer_enum(
        self,
        table_name: str,
        column_name: str,
        current_type: str,
        stats: ColumnStatistics,
    ) -> TypeRecommendation | None:
        """Check if column should be an enum."""
        # Low cardinality check
        if stats.cardinality > self.ENUM_CARDINALITY_THRESHOLD:
            return None

        if stats.cardinality_ratio > self.ENUM_CARDINALITY_RATIO:
            return None

        # At least minimum distinct values
        if stats.cardinality < self.MIN_DISTINCT_VALUES:
            return None

        # Calculate confidence based on cardinality
        confidence = self._calculate_enum_confidence(stats)

        return TypeRecommendation(
            table_name=table_name,
            column_name=column_name,
            current_type=current_type,
            inferred_type=InferredType.ENUM,
            confidence=confidence,
            evidence={
                "cardinality": stats.cardinality,
                "total_rows": stats.total_rows,
                "cardinality_ratio": round(stats.cardinality_ratio, 4),
            },
            enum_values={str(v) for v in stats.value_counts},
        )

    def _infer_boolean(
        self,
        table_name: str,
        column_name: str,
        current_type: str,
        stats: ColumnStatistics,
    ) -> TypeRecommendation | None:
        """Check if column should be a boolean."""
        # Must have exactly 2 unique values
        if stats.cardinality != self.BOOLEAN_VALUE_COUNT:
            return None

        # Check if values look like booleans
        values = set(stats.value_counts.keys())

        # Convert to strings for comparison (handles both numeric and string values)
        str_values = {str(v).lower() for v in values}

        # Also check numeric values directly for patterns like {-1, 0}
        is_boolean_like = (
            values in BOOLEAN_VALUE_SETS or str_values in BOOLEAN_VALUE_SETS
        )

        if not is_boolean_like:
            return None

        # High confidence if pattern matches
        pattern_match_ratio = stats.boolean_pattern_matches / stats.non_null_count
        confidence = min(pattern_match_ratio + 0.5, 1.0)

        return TypeRecommendation(
            table_name=table_name,
            column_name=column_name,
            current_type=current_type,
            inferred_type=InferredType.BOOLEAN,
            confidence=confidence,
            evidence={
                "values": sorted(str(v) for v in values),
                "pattern_matches": stats.boolean_pattern_matches,
                "pattern_match_ratio": round(pattern_match_ratio, 3),
            },
        )

    def _infer_date(
        self,
        table_name: str,
        column_name: str,
        current_type: str,
        stats: ColumnStatistics,
    ) -> TypeRecommendation | None:
        """Check if column should be a date."""
        if stats.date_pattern_matches == 0:
            return None

        pattern_match_ratio = stats.date_pattern_matches / stats.non_null_count

        if pattern_match_ratio < self.PATTERN_MATCH_THRESHOLD:
            return None

        # Find most common format
        detected_format = self._get_most_common_format(stats.detected_formats)

        confidence = pattern_match_ratio

        return TypeRecommendation(
            table_name=table_name,
            column_name=column_name,
            current_type=current_type,
            inferred_type=InferredType.DATE,
            confidence=confidence,
            evidence={
                "pattern_matches": stats.date_pattern_matches,
                "pattern_match_ratio": round(pattern_match_ratio, 3),
                "formats": stats.detected_formats,
            },
            detected_format=detected_format,
        )

    def _infer_datetime(
        self,
        table_name: str,
        column_name: str,
        current_type: str,
        stats: ColumnStatistics,
    ) -> TypeRecommendation | None:
        """Check if column should be a datetime."""
        if stats.datetime_pattern_matches == 0:
            return None

        pattern_match_ratio = stats.datetime_pattern_matches / stats.non_null_count

        if pattern_match_ratio < self.PATTERN_MATCH_THRESHOLD:
            return None

        # Find most common datetime format
        detected_format = self._get_most_common_format(
            stats.detected_formats,
            filter_key="datetime",
        )

        confidence = pattern_match_ratio

        return TypeRecommendation(
            table_name=table_name,
            column_name=column_name,
            current_type=current_type,
            inferred_type=InferredType.DATETIME,
            confidence=confidence,
            evidence={
                "pattern_matches": stats.datetime_pattern_matches,
                "pattern_match_ratio": round(pattern_match_ratio, 3),
                "formats": stats.detected_formats,
            },
            detected_format=detected_format,
        )

    def _infer_uuid(
        self,
        table_name: str,
        column_name: str,
        current_type: str,
        stats: ColumnStatistics,
    ) -> TypeRecommendation | None:
        """Check if column should be a UUID."""
        if stats.uuid_pattern_matches == 0:
            return None

        pattern_match_ratio = stats.uuid_pattern_matches / stats.non_null_count

        if pattern_match_ratio < self.PATTERN_MATCH_THRESHOLD:
            return None

        confidence = pattern_match_ratio

        return TypeRecommendation(
            table_name=table_name,
            column_name=column_name,
            current_type=current_type,
            inferred_type=InferredType.UUID,
            confidence=confidence,
            evidence={
                "pattern_matches": stats.uuid_pattern_matches,
                "pattern_match_ratio": round(pattern_match_ratio, 3),
            },
        )

    def _calculate_enum_confidence(self, stats: ColumnStatistics) -> float:
        """Calculate confidence score for enum recommendation.

        Lower cardinality and more data = higher confidence.
        """
        # Base confidence from cardinality
        cardinality_score = 1.0 - (stats.cardinality / self.ENUM_CARDINALITY_THRESHOLD)

        # Boost confidence if we have lots of data
        data_score = (
            min(stats.total_rows / self.ENUM_DATA_THRESHOLD, 1.0) * self.ENUM_DATA_BOOST
        )

        # Reduce confidence for high cardinality ratio
        ratio_penalty = stats.cardinality_ratio * self.ENUM_RATIO_PENALTY

        confidence = cardinality_score + data_score - ratio_penalty

        return max(min(confidence, 1.0), 0.0)
