"""Report generation for analysis results."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

from jinja2 import Environment, FileSystemLoader, select_autoescape

if TYPE_CHECKING:
    from .types import NamePattern, TypeRecommendation

TEMPLATE_DIR = Path(__file__).parent / "templates"


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
        """Initialize with analysis results."""
        self.recommendations = recommendations
        self.patterns = patterns
        self.database_name = database_name

    def generate_json(self, output_path: Path) -> None:
        """Generate JSON report."""

        # Custom dict factory to handle sets (convert to sorted lists)
        def dict_factory(fields: list[tuple[str, Any]]) -> dict[str, Any]:
            return {k: sorted(v) if isinstance(v, set) and v else v for k, v in fields}

        report = {
            "database": self.database_name,
            "generated_at": datetime.now(UTC).isoformat(),
            "summary": self._generate_summary(),
            "recommendations": [
                asdict(rec, dict_factory=dict_factory) for rec in self.recommendations
            ],
            "patterns": [
                asdict(pat, dict_factory=dict_factory) for pat in self.patterns
            ],
        }

        output_path.write_text(json.dumps(report, indent=2))

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
