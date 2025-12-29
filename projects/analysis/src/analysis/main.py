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


def _create_engine_url(database: Path | str) -> str:
    """Create SQLAlchemy URL for database connection."""
    # If it's already a URL string, use it directly
    if isinstance(database, str) and "://" in database:
        return database

    # Convert to Path for file-based databases
    db_path = Path(database) if isinstance(database, str) else database

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    # Detect database type from extension
    suffix = db_path.suffix.lower()

    if suffix in {".sqlite", ".db", ".sqlite3"}:
        return f"sqlite:///{db_path}"
    if suffix in {".mdb", ".accdb"}:
        # Access database connection string for Windows
        abs_path = db_path.resolve()
        driver = "Microsoft Access Driver (*.mdb, *.accdb)"
        return f"access+pyodbc:///?odbc_connect=DRIVER={{{driver}}};DBQ={abs_path}"
    print(
        f"Error: Unsupported database extension: {suffix}. "
        f"Supported: .sqlite, .db, .sqlite3, .mdb, .accdb",
        file=sys.stderr,
    )
    sys.exit(1)


def analyze_database(
    database: Path | str,
    *,
    confidence_threshold: float = 0.7,
    output_format: str = "json",
    output_path: Path | None = None,
) -> None:
    """Analyze database for type refinement opportunities."""
    # Create engine
    engine_url = _create_engine_url(database)
    engine = create_engine(engine_url)

    # Get database name for display and output file naming
    db_name = Path(database).stem if isinstance(database, (str, Path)) else "database"
    print(f"Analyzing database: {db_name}", file=sys.stderr)

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
        f"Found {len(all_recommendations)} recommendations",
        file=sys.stderr,
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
        database_name=db_name,
    )

    if output_path is None:
        output_path = Path(f"analysis-{db_name}.{output_format}")

    print(f"Generating report: {output_path}", file=sys.stderr)

    if output_format == "json":
        report.generate_json(output_path)
    elif output_format == "markdown":
        report.generate_markdown(output_path)
    else:
        print(f"Error: Unknown format: {output_format}", file=sys.stderr)
        sys.exit(1)

    print(f"Analysis complete! Report written to {output_path}", file=sys.stderr)
