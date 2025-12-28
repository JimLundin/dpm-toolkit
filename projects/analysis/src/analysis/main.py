"""Main analysis module for type refinement discovery."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import create_engine

from .inference import TypeInferenceEngine
from .pattern_mining import PatternMiner
from .reporting import AnalysisReport
from .statistics import StatisticsCollector

if TYPE_CHECKING:
    from collections.abc import Generator

    from .types import TypeRecommendation


def analyze_database(
    database_path: Path,
    *,
    confidence_threshold: float = 0.7,
    output_format: str = "json",
    output_path: Path | None = None,
) -> None:
    """Analyze SQLite database for type refinement opportunities."""
    if not database_path.exists():
        print(f"Error: Database not found: {database_path}", file=sys.stderr)
        sys.exit(1)

    # Create engine
    engine = create_engine(f"sqlite:///{database_path}")

    print(f"Analyzing database: {database_path.name}", file=sys.stderr)

    # Collect statistics
    collector = StatisticsCollector(engine)
    inference_engine = TypeInferenceEngine()

    # Analyze all tables and columns
    def analyze_tables() -> Generator[TypeRecommendation]:
        for table_name in collector.metadata.tables:
            print(f"  Analyzing table: {table_name}...", file=sys.stderr)
            table_stats = collector.collect_table_statistics(table_name)
            table = collector.metadata.tables[table_name]

            for column in table.columns:
                if column.name in table_stats:
                    recommendation = inference_engine.infer_type(
                        table_name=table_name,
                        column_name=column.name,
                        current_type=str(column.type),
                        stats=table_stats[column.name],
                    )
                    meets_threshold = (
                        recommendation
                        and recommendation.confidence >= confidence_threshold
                    )
                    if meets_threshold:
                        yield recommendation

    all_recommendations = list(analyze_tables())

    print(
        f"Found {len(all_recommendations)} recommendations", file=sys.stderr,
    )

    # Mine patterns
    print("Mining naming patterns...", file=sys.stderr)
    pattern_miner = PatternMiner()
    patterns = pattern_miner.mine_patterns(all_recommendations)
    print(f"Discovered {len(patterns)} patterns", file=sys.stderr)

    # Generate report
    report = AnalysisReport(
        recommendations=all_recommendations,
        patterns=patterns,
        database_name=database_path.stem,
    )

    if output_path is None:
        output_path = Path(f"analysis-{database_path.stem}.{output_format}")

    print(f"Generating report: {output_path}", file=sys.stderr)

    if output_format == "json":
        report.generate_json(output_path)
    elif output_format == "markdown":
        report.generate_markdown(output_path)
    else:
        print(f"Error: Unknown format: {output_format}", file=sys.stderr)
        sys.exit(1)

    print(f"Analysis complete! Report written to {output_path}", file=sys.stderr)
