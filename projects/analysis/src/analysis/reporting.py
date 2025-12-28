"""Report generation for analysis results."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypedDict

from .types import InferredType, NamePattern, TypeRecommendation

if TYPE_CHECKING:
    from pathlib import Path


class SummaryData(TypedDict):
    """Structure for summary statistics."""

    total_recommendations: int
    by_type: dict[str, int]
    by_confidence: dict[str, int]
    patterns_discovered: int


class AnalysisReport:
    """Generates reports from analysis results."""

    # Report generation constants
    HIGH_CONFIDENCE_THRESHOLD = 0.9
    MEDIUM_CONFIDENCE_THRESHOLD = 0.7
    MAX_ENUM_VALUES_DISPLAY = 20
    MAX_RECOMMENDATIONS_DISPLAY = 20

    def __init__(
        self,
        recommendations: list[TypeRecommendation],
        patterns: list[NamePattern],
        database_name: str,
    ) -> None:
        """Initialize the report generator.

        Args:
            recommendations: List of type recommendations
            patterns: List of discovered naming patterns
            database_name: Name/version of the analyzed database

        """
        self.recommendations = recommendations
        self.patterns = patterns
        self.database_name = database_name

    def generate_json(self, output_path: Path) -> None:
        """Generate JSON report.

        Args:
            output_path: Path to write the JSON report

        """
        report = {
            "database": self.database_name,
            "generated_at": datetime.now(UTC).isoformat(),
            "summary": self._generate_summary(),
            "recommendations": [rec.to_dict() for rec in self.recommendations],
            "patterns": [pat.to_dict() for pat in self.patterns],
        }

        output_path.write_text(json.dumps(report, indent=2))

    def generate_markdown(self, output_path: Path) -> None:
        """Generate Markdown report.

        Args:
            output_path: Path to write the Markdown report

        """
        lines = [
            "# Type Refinement Analysis Report",
            "",
            f"**Database:** {self.database_name}",
            f"**Generated:** {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "",
            self._generate_summary_section(),
            self._generate_patterns_section(),
            self._generate_recommendations_section(),
        ]

        output_path.write_text("\n".join(lines))

    def _generate_summary(self) -> SummaryData:
        """Generate summary statistics."""
        by_type: dict[str, int] = defaultdict(int)
        by_confidence: dict[str, int] = {"high": 0, "medium": 0, "low": 0}

        for rec in self.recommendations:
            by_type[rec.inferred_type] += 1

            if rec.confidence >= self.HIGH_CONFIDENCE_THRESHOLD:
                by_confidence["high"] += 1
            elif rec.confidence >= self.MEDIUM_CONFIDENCE_THRESHOLD:
                by_confidence["medium"] += 1
            else:
                by_confidence["low"] += 1

        return SummaryData(
            total_recommendations=len(self.recommendations),
            by_type=dict(by_type),
            by_confidence=by_confidence,
            patterns_discovered=len(self.patterns),
        )

    def _generate_summary_section(self) -> str:
        """Generate summary section for Markdown."""
        summary = self._generate_summary()

        lines = [
            "## Summary",
            "",
            f"- **Total Recommendations:** {summary['total_recommendations']}",
            f"- **High Confidence (≥0.9):** {summary['by_confidence']['high']}",
            f"- **Medium Confidence (0.7-0.9):** {summary['by_confidence']['medium']}",
            f"- **Low Confidence (<0.7):** {summary['by_confidence']['low']}",
            "",
            "### By Type",
            "",
        ]

        for type_name, count in sorted(summary["by_type"].items()):
            lines.append(f"- **{type_name}:** {count}")

        lines.append("")
        return "\n".join(lines)

    def _generate_patterns_section(self) -> str:
        """Generate patterns section for Markdown."""
        if not self.patterns:
            return "## Discovered Naming Patterns\n\nNo patterns discovered.\n"

        lines = [
            "## Discovered Naming Patterns",
            "",
            (
                "These patterns can be added to `type_registry.py` to improve "
                "future migrations:"
            ),
            "",
        ]

        # Group patterns by type
        by_type: dict[InferredType, list[NamePattern]] = defaultdict(list)
        for pattern in self.patterns:
            by_type[pattern.inferred_type].append(pattern)

        for inferred_type, patterns in sorted(by_type.items()):
            lines.append(f"### {inferred_type.value.upper()} Patterns")
            lines.append("")

            # Show top 5 patterns per type
            for pattern in sorted(patterns, key=lambda p: p.confidence, reverse=True)[
                :5
            ]:
                lines.append(
                    f"#### {pattern.pattern_type.capitalize()}: `{pattern.pattern}`",
                )
                lines.append("")
                lines.append(
                    f"- **Occurrences:** {pattern.occurrences} "
                    f"({pattern.confidence * 100:.1f}% confidence)",
                )
                lines.append(f"- **Examples:** {', '.join(pattern.examples)}")
                lines.append("")

                # Suggest code change
                lines.append("**Suggested addition to `type_registry.py`:**")
                lines.append("```python")

                if pattern.pattern_type == "suffix":
                    lines.append(f'if col_name.lower().endswith("{pattern.pattern}"):')
                    lines.append(f"    return {inferred_type.upper()}")
                elif pattern.pattern_type == "prefix":
                    lines.append(
                        f'if col_name.lower().startswith("{pattern.pattern}"):',
                    )
                    lines.append(f"    return {inferred_type.upper()}")
                elif pattern.pattern_type == "exact":
                    lines.append(f'if col_name.lower() == "{pattern.pattern}":')
                    lines.append(f"    return {inferred_type.upper()}")

                lines.append("```")
                lines.append("")

        return "\n".join(lines)

    def _generate_recommendations_section(self) -> str:  # noqa: C901
        """Generate recommendations section for Markdown."""
        if not self.recommendations:
            return "## Recommendations\n\nNo recommendations found.\n"

        lines = [
            "## Detailed Recommendations",
            "",
        ]

        # Group by inferred type
        by_type: dict[InferredType, list[TypeRecommendation]] = defaultdict(list)
        for rec in self.recommendations:
            by_type[rec.inferred_type].append(rec)

        for inferred_type, recs in sorted(by_type.items()):
            lines.append(f"### {inferred_type.value.upper()} Candidates")
            lines.append("")

            # Sort by confidence, show top recommendations
            sorted_recs = sorted(recs, key=lambda r: r.confidence, reverse=True)[
                : self.MAX_RECOMMENDATIONS_DISPLAY
            ]

            for rec in sorted_recs:
                lines.append(f"#### {rec.table_name}.{rec.column_name}")
                lines.append("")
                lines.append(f"- **Current Type:** {rec.current_type}")
                lines.append(f"- **Confidence:** {rec.confidence * 100:.1f}%")

                if rec.inferred_type == InferredType.ENUM and rec.enum_values:
                    values_str = ", ".join(
                        sorted(rec.enum_values)[: self.MAX_ENUM_VALUES_DISPLAY],
                    )
                    if len(rec.enum_values) > self.MAX_ENUM_VALUES_DISPLAY:
                        values_str += f" ... ({len(rec.enum_values)} total)"
                    lines.append(f"- **Values:** {values_str}")

                if rec.detected_format:
                    lines.append(f"- **Format:** {rec.detected_format}")

                # Show evidence
                if rec.evidence:
                    lines.append("- **Evidence:**")
                    for key, value in rec.evidence.items():
                        lines.append(f"  - {key}: {value}")

                lines.append("")

            if len(recs) > self.MAX_RECOMMENDATIONS_DISPLAY:
                lines.append(
                    f"*Showing top {self.MAX_RECOMMENDATIONS_DISPLAY} of "
                    f"{len(recs)} {inferred_type.value} candidates*",
                )
                lines.append("")

        return "\n".join(lines)
