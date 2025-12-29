"""Report generation for analysis results."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .types import AnalysisReport, ReportSummary

if TYPE_CHECKING:
    from .types import NamePattern, TypeRecommendation

TEMPLATE_DIR = Path(__file__).parent / "templates"


def _json_default(obj: object) -> object:
    """Convert non-serializable objects for JSON encoding."""
    if isinstance(obj, set):
        return sorted(obj) if obj else []
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)


class ReportGenerator:
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
        """Initialize with analysis results."""
        self.recommendations = recommendations
        self.patterns = patterns
        self.database_name = database_name

    def generate_json(self, output_path: Path) -> None:
        """Generate JSON report."""
        # Create report dataclass
        report = AnalysisReport(
            database=self.database_name,
            generated_at=datetime.now(UTC).isoformat(),
            summary=self._generate_summary(),
            recommendations=self.recommendations,
            patterns=self.patterns,
        )

        # Convert to dict using asdict()
        report_dict = asdict(report)

        # Serialize to JSON with custom default handler for sets
        output_path.write_text(json.dumps(report_dict, indent=2, default=_json_default))

    def generate_markdown(self, output_path: Path) -> None:
        """Generate Markdown report using Jinja2 template."""
        env = Environment(
            loader=FileSystemLoader(TEMPLATE_DIR),
            autoescape=select_autoescape(),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        template = env.get_template("report.md")

        # Prepare data for template
        summary = self._generate_summary()

        # Group recommendations and patterns by type
        recommendations_by_type: dict[str, list[TypeRecommendation]] = defaultdict(list)
        for rec in self.recommendations:
            recommendations_by_type[rec.inferred_type.value].append(rec)

        # Sort each group by confidence
        for type_recs in recommendations_by_type.values():
            type_recs.sort(key=lambda r: r.confidence, reverse=True)

        patterns_by_type: dict[str, list[NamePattern]] = defaultdict(list)
        for pat in self.patterns:
            patterns_by_type[pat.inferred_type.value].append(pat)

        # Sort patterns by confidence
        for type_pats in patterns_by_type.values():
            type_pats.sort(key=lambda p: p.confidence, reverse=True)

        rendered = template.render(
            database_name=self.database_name,
            generated_at=datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
            summary=summary,
            recommendations_by_type=recommendations_by_type,
            patterns_by_type=patterns_by_type,
            max_recommendations=self.MAX_RECOMMENDATIONS_DISPLAY,
            max_enum_values=self.MAX_ENUM_VALUES_DISPLAY,
            recommendations=self.recommendations,
            patterns=self.patterns,
        )

        output_path.write_text(rendered)

    def _generate_summary(self) -> ReportSummary:
        """Generate summary statistics."""
        by_type: dict[str, int] = defaultdict(int)
        by_pattern_type: dict[str, int] = defaultdict(int)

        for rec in self.recommendations:
            by_type[rec.inferred_type] += 1

        for pat in self.patterns:
            by_pattern_type[pat.pattern_type] += 1

        return ReportSummary(
            total_recommendations=len(self.recommendations),
            by_type=dict(by_type),
            total_patterns=len(self.patterns),
            by_pattern_type=dict(by_pattern_type),
        )
