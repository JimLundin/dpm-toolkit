"""Pattern mining for discovering naming conventions."""

from __future__ import annotations

from collections import defaultdict
from itertools import chain

from .types import InferredType, NamePattern, TypeRecommendation


class PatternMiner:
    """Discovers naming patterns from type recommendations."""

    MIN_OCCURRENCES = 3  # Minimum occurrences to consider a pattern
    MIN_SUFFIX_LENGTH = 3  # Minimum suffix length
    MAX_SUFFIX_LENGTH = 15  # Maximum suffix length
    MIN_PREFIX_LENGTH = 2  # Minimum prefix length
    MAX_PREFIX_LENGTH = 10  # Maximum prefix length

    def mine_patterns(
        self, recommendations: list[TypeRecommendation],
    ) -> list[NamePattern]:
        """Find common naming patterns in type recommendations."""
        # Group recommendations by inferred type
        by_type: dict[InferredType, list[TypeRecommendation]] = defaultdict(list)
        for rec in recommendations:
            by_type[rec.inferred_type].append(rec)

        # Extract all pattern types for each inferred type
        patterns = list(
            chain.from_iterable(
                chain(
                    self._extract_suffix_patterns(
                        [r.column_name for r in recs],
                        inferred_type,
                    ),
                    self._extract_prefix_patterns(
                        [r.column_name for r in recs],
                        inferred_type,
                    ),
                    self._extract_exact_patterns(
                        [r.column_name for r in recs],
                        inferred_type,
                    ),
                )
                for inferred_type, recs in by_type.items()
            ),
        )

        return sorted(patterns, key=lambda p: p.confidence, reverse=True)

    def _extract_suffix_patterns(
        self, column_names: list[str], inferred_type: InferredType,
    ) -> list[NamePattern]:
        """Extract common suffix patterns from column names."""
        suffix_counts: dict[str, list[str]] = defaultdict(list)

        # Try different suffix lengths
        for name in column_names:
            name_lower = name.lower()
            max_length = min(len(name_lower), self.MAX_SUFFIX_LENGTH) + 1
            for length in range(self.MIN_SUFFIX_LENGTH, max_length):
                suffix = name_lower[-length:]
                suffix_counts[suffix].append(name)

        # Filter and create patterns using comprehension
        return [
            NamePattern(
                pattern_type="suffix",
                pattern=suffix,
                inferred_type=inferred_type,
                occurrences=len(examples),
                confidence=min(
                    len(examples) / len(column_names)
                    + min(len(suffix) / self.MAX_SUFFIX_LENGTH, 1.0) * 0.1,
                    1.0,
                ),
                examples=examples[:5],
            )
            for suffix, examples in suffix_counts.items()
            if len(examples) >= self.MIN_OCCURRENCES
        ]

    def _extract_prefix_patterns(
        self, column_names: list[str], inferred_type: InferredType,
    ) -> list[NamePattern]:
        """Extract common prefix patterns from column names."""
        prefix_counts: dict[str, list[str]] = defaultdict(list)

        # Try different prefix lengths
        for name in column_names:
            name_lower = name.lower()
            max_length = min(len(name_lower), self.MAX_PREFIX_LENGTH) + 1
            for length in range(self.MIN_PREFIX_LENGTH, max_length):
                prefix = name_lower[:length]
                prefix_counts[prefix].append(name)

        # Filter and create patterns using comprehension
        return [
            NamePattern(
                pattern_type="prefix",
                pattern=prefix,
                inferred_type=inferred_type,
                occurrences=len(examples),
                confidence=min(
                    len(examples) / len(column_names)
                    + min(len(prefix) / self.MAX_PREFIX_LENGTH, 1.0) * 0.1,
                    1.0,
                ),
                examples=examples[:5],
            )
            for prefix, examples in prefix_counts.items()
            if len(examples) >= self.MIN_OCCURRENCES
        ]

    def _extract_exact_patterns(
        self, column_names: list[str], inferred_type: InferredType,
    ) -> list[NamePattern]:
        """Extract exact match patterns for frequently occurring names."""
        name_counts: dict[str, int] = defaultdict(int)

        for name in column_names:
            name_lower = name.lower()
            name_counts[name_lower] += 1

        # Create patterns using comprehension
        return [
            NamePattern(
                pattern_type="exact",
                pattern=name,
                inferred_type=inferred_type,
                occurrences=count,
                confidence=min(count / len(column_names) + 0.5, 1.0),
                examples=[name],
            )
            for name, count in name_counts.items()
            if count >= self.MIN_OCCURRENCES
        ]
