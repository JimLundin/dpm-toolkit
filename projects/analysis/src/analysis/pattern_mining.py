"""Pattern mining for discovering naming conventions."""

from __future__ import annotations

from collections import Counter, defaultdict
from itertools import chain

from .types import InferredType, NamePattern, TypeRecommendation


class PatternMiner:
    """Discovers naming patterns from type recommendations."""

    MIN_OCCURRENCES = 3  # Minimum occurrences to consider a pattern
    MIN_SUFFIX_LENGTH = 3  # Minimum suffix length
    MAX_SUFFIX_LENGTH = 15  # Maximum suffix length
    MIN_PREFIX_LENGTH = 2  # Minimum prefix length
    MAX_PREFIX_LENGTH = 10  # Maximum prefix length

    # Preferred affix lengths to check (most patterns found at these lengths)
    # Reduces complexity from O(n×m) to O(n×k) where k is constant
    PREFERRED_SUFFIX_LENGTHS = [3, 4, 5]  # Common suffix lengths
    PREFERRED_PREFIX_LENGTHS = [2, 3, 4]  # Common prefix lengths

    def mine_patterns(
        self,
        recommendations: list[TypeRecommendation],
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

    def _extract_affix_patterns(
        self,
        column_names: list[str],
        inferred_type: InferredType,
        pattern_type: str,
        min_length: int,
        max_length: int,
    ) -> list[NamePattern]:
        """Extract common affix (prefix or suffix) patterns from column names.

        Args:
            column_names: List of column names to analyze
            inferred_type: The inferred type for these columns
            pattern_type: Either "prefix" or "suffix"
            min_length: Minimum length of affix to consider
            max_length: Maximum length of affix to consider

        Returns:
            List of discovered name patterns

        """
        if not column_names:
            return []

        affix_counts: dict[str, list[str]] = defaultdict(list)
        is_suffix = pattern_type == "suffix"

        # Use preferred lengths for better performance (O(n×k) instead of O(n×m))
        # Most meaningful patterns are captured at common lengths
        preferred_lengths = (
            self.PREFERRED_SUFFIX_LENGTHS
            if is_suffix
            else self.PREFERRED_PREFIX_LENGTHS
        )

        # Try preferred affix lengths only
        for name in column_names:
            name_lower = name.lower()
            for length in preferred_lengths:
                # Skip if name is too short for this length
                if length > len(name_lower):
                    continue
                affix = name_lower[-length:] if is_suffix else name_lower[:length]
                affix_counts[affix].append(name)

        # Filter and create patterns using comprehension
        return [
            NamePattern(
                pattern_type=pattern_type,
                pattern=affix,
                inferred_type=inferred_type,
                occurrences=len(examples),
                confidence=min(
                    len(examples) / len(column_names)
                    + min(len(affix) / max_length, 1.0) * 0.1,
                    1.0,
                ),
                examples=examples[:5],
            )
            for affix, examples in affix_counts.items()
            if len(examples) >= self.MIN_OCCURRENCES
        ]

    def _extract_suffix_patterns(
        self,
        column_names: list[str],
        inferred_type: InferredType,
    ) -> list[NamePattern]:
        """Extract common suffix patterns from column names."""
        return self._extract_affix_patterns(
            column_names,
            inferred_type,
            "suffix",
            self.MIN_SUFFIX_LENGTH,
            self.MAX_SUFFIX_LENGTH,
        )

    def _extract_prefix_patterns(
        self,
        column_names: list[str],
        inferred_type: InferredType,
    ) -> list[NamePattern]:
        """Extract common prefix patterns from column names."""
        return self._extract_affix_patterns(
            column_names,
            inferred_type,
            "prefix",
            self.MIN_PREFIX_LENGTH,
            self.MAX_PREFIX_LENGTH,
        )

    def _extract_exact_patterns(
        self,
        column_names: list[str],
        inferred_type: InferredType,
    ) -> list[NamePattern]:
        """Extract exact match patterns for frequently occurring names."""
        if not column_names:
            return []

        name_counts = Counter(name.lower() for name in column_names)

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
