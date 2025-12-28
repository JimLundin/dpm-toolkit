"""Pattern mining for discovering naming conventions."""

from __future__ import annotations

from collections import defaultdict

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
        """Find common naming patterns in recommended type changes.

        Args:
            recommendations: List of type recommendations

        Returns:
            List of discovered naming patterns, sorted by confidence

        """
        patterns: list[NamePattern] = []

        # Group recommendations by inferred type
        by_type: dict[InferredType, list[TypeRecommendation]] = defaultdict(list)
        for rec in recommendations:
            by_type[rec.inferred_type].append(rec)

        # Mine patterns for each type
        for inferred_type, recs in by_type.items():
            column_names = [rec.column_name for rec in recs]

            # Extract suffix patterns
            suffix_patterns = self._extract_suffix_patterns(
                column_names, inferred_type,
            )
            patterns.extend(suffix_patterns)

            # Extract prefix patterns
            prefix_patterns = self._extract_prefix_patterns(
                column_names, inferred_type,
            )
            patterns.extend(prefix_patterns)

            # Extract exact match patterns (for very common names)
            exact_patterns = self._extract_exact_patterns(
                column_names, inferred_type,
            )
            patterns.extend(exact_patterns)

        # Sort by confidence (descending)
        return sorted(patterns, key=lambda p: p.confidence, reverse=True)

    def _extract_suffix_patterns(
        self, column_names: list[str], inferred_type: InferredType,
    ) -> list[NamePattern]:
        """Extract common suffix patterns from column names.

        Args:
            column_names: List of column names
            inferred_type: The type these columns are inferred as

        Returns:
            List of suffix patterns

        """
        suffix_counts: dict[str, list[str]] = defaultdict(list)

        # Try different suffix lengths
        for name in column_names:
            name_lower = name.lower()
            for length in range(
                self.MIN_SUFFIX_LENGTH,
                min(len(name_lower), self.MAX_SUFFIX_LENGTH) + 1,
            ):
                suffix = name_lower[-length:]
                suffix_counts[suffix].append(name)

        # Filter and create patterns
        patterns: list[NamePattern] = []
        for suffix, examples in suffix_counts.items():
            if len(examples) >= self.MIN_OCCURRENCES:
                # Calculate confidence based on frequency
                confidence = min(len(examples) / len(column_names), 1.0)

                # Prefer longer, more specific suffixes
                specificity_boost = min(len(suffix) / self.MAX_SUFFIX_LENGTH, 1.0) * 0.1
                confidence = min(confidence + specificity_boost, 1.0)

                patterns.append(
                    NamePattern(
                        pattern_type="suffix",
                        pattern=suffix,
                        inferred_type=inferred_type,
                        occurrences=len(examples),
                        confidence=confidence,
                        examples=examples[:5],  # Store first 5 examples
                    ),
                )

        return patterns

    def _extract_prefix_patterns(
        self, column_names: list[str], inferred_type: InferredType,
    ) -> list[NamePattern]:
        """Extract common prefix patterns from column names.

        Args:
            column_names: List of column names
            inferred_type: The type these columns are inferred as

        Returns:
            List of prefix patterns

        """
        prefix_counts: dict[str, list[str]] = defaultdict(list)

        # Try different prefix lengths
        for name in column_names:
            name_lower = name.lower()
            for length in range(
                self.MIN_PREFIX_LENGTH,
                min(len(name_lower), self.MAX_PREFIX_LENGTH) + 1,
            ):
                prefix = name_lower[:length]
                prefix_counts[prefix].append(name)

        # Filter and create patterns
        patterns: list[NamePattern] = []
        for prefix, examples in prefix_counts.items():
            if len(examples) >= self.MIN_OCCURRENCES:
                # Calculate confidence
                confidence = min(len(examples) / len(column_names), 1.0)

                # Prefer longer, more specific prefixes
                specificity_boost = min(len(prefix) / self.MAX_PREFIX_LENGTH, 1.0) * 0.1
                confidence = min(confidence + specificity_boost, 1.0)

                patterns.append(
                    NamePattern(
                        pattern_type="prefix",
                        pattern=prefix,
                        inferred_type=inferred_type,
                        occurrences=len(examples),
                        confidence=confidence,
                        examples=examples[:5],
                    ),
                )

        return patterns

    def _extract_exact_patterns(
        self, column_names: list[str], inferred_type: InferredType,
    ) -> list[NamePattern]:
        """Extract exact match patterns for frequently occurring names.

        Args:
            column_names: List of column names
            inferred_type: The type these columns are inferred as

        Returns:
            List of exact match patterns

        """
        name_counts: dict[str, int] = defaultdict(int)

        for name in column_names:
            name_lower = name.lower()
            name_counts[name_lower] += 1

        # Create patterns for names that appear multiple times
        patterns: list[NamePattern] = []
        for name, count in name_counts.items():
            if count >= self.MIN_OCCURRENCES:
                # High confidence for exact matches
                confidence = min(count / len(column_names) + 0.5, 1.0)

                patterns.append(
                    NamePattern(
                        pattern_type="exact",
                        pattern=name,
                        inferred_type=inferred_type,
                        occurrences=count,
                        confidence=confidence,
                        examples=[name],
                    ),
                )

        return patterns
