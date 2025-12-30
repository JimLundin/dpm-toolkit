"""Main analysis module for type refinement discovery."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict
from typing import TYPE_CHECKING

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import create_engine

from .inference import TypeInferenceEngine
from .pattern_mining import PatternMiner
from .reporting import TEMPLATE_DIR, _json_default
from .statistics import StatisticsCollector

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from sqlalchemy.engine import Engine

    from .types import AnalysisReport, NamePattern, TypeRecommendation

# Display limits for markdown reports
MAX_RECOMMENDATIONS_DISPLAY = 20
MAX_ENUM_VALUES_DISPLAY = 20

# Jinja2 environment for markdown template rendering
_JINJA_ENV = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(),
    trim_blocks=True,
    lstrip_blocks=True,
)


def create_engine_for_database(database: Path) -> Engine:
    """Create SQLAlchemy engine for a database file with connection pooling.

    Args:
        database: Path to the database file (.sqlite, .db, .sqlite3, .mdb, .accdb)

    Returns:
        Configured SQLAlchemy engine with connection pooling

    Raises:
        ValueError: If database extension is not supported

    """
    suffix = database.suffix.lower()

    # Connection pool configuration for better resource management
    pool_config = {
        "pool_size": 5,  # Maintain 5 connections in pool
        "max_overflow": 10,  # Allow up to 10 additional connections
        "pool_pre_ping": True,  # Verify connections before use
    }

    if suffix in {".sqlite", ".db", ".sqlite3"}:
        return create_engine(f"sqlite:///{database}", **pool_config)

    if suffix in {".mdb", ".accdb"}:
        abs_path = database.resolve()
        driver = "Microsoft Access Driver (*.mdb, *.accdb)"
        connection_string = f"DRIVER={{{driver}}};DBQ={abs_path}"
        return create_engine(
            f"access+pyodbc:///?odbc_connect={connection_string}",
            **pool_config,
        )

    msg = f"Unsupported database extension: {suffix}"
    raise ValueError(msg)


def analyze_database(
    engine: Engine,
    *,
    confidence_threshold: float = 0.7,
) -> tuple[list[TypeRecommendation], list[NamePattern]]:
    """Analyze database for type refinement opportunities.

    Args:
        engine: SQLAlchemy engine connected to the database
        confidence_threshold: Minimum confidence threshold for recommendations

    Returns:
        Tuple of (recommendations, patterns)

    """
    # Collect statistics and analyze
    collector = StatisticsCollector(engine)
    inference_engine = TypeInferenceEngine()

    # Analyze all tables and columns
    def analyze_tables() -> Iterator[TypeRecommendation]:
        for table_name in collector.metadata.tables:
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
                    if (
                        recommendation is not None
                        and recommendation.confidence >= confidence_threshold
                    ):
                        yield recommendation

    recommendations = list(analyze_tables())

    # Mine patterns
    pattern_miner = PatternMiner()
    patterns = pattern_miner.mine_patterns(recommendations)

    return recommendations, patterns


def report_to_json(report: AnalysisReport) -> str:
    """Convert AnalysisReport to JSON string.

    Args:
        report: AnalysisReport dataclass

    Returns:
        JSON string representation

    """
    report_dict = asdict(report)
    return json.dumps(report_dict, indent=2, default=_json_default)


def report_to_markdown(report: AnalysisReport) -> str:
    """Convert AnalysisReport to Markdown string.

    Args:
        report: AnalysisReport dataclass

    Returns:
        Markdown string representation

    """
    template = _JINJA_ENV.get_template("report.md")

    # Group recommendations by type and sort by confidence
    recommendations_by_type = defaultdict(list)
    for rec in report.recommendations:
        recommendations_by_type[rec.inferred_type.value].append(rec)

    for recs in recommendations_by_type.values():
        recs.sort(key=lambda r: r.confidence, reverse=True)

    # Group patterns by type and sort by confidence
    patterns_by_type = defaultdict(list)
    for pat in report.patterns:
        patterns_by_type[pat.inferred_type.value].append(pat)

    for pats in patterns_by_type.values():
        pats.sort(key=lambda p: p.confidence, reverse=True)

    return template.render(
        database_name=report.database,
        generated_at=report.generated_at,
        summary=report.summary,
        recommendations_by_type=recommendations_by_type,
        patterns_by_type=patterns_by_type,
        max_recommendations=MAX_RECOMMENDATIONS_DISPLAY,
        max_enum_values=MAX_ENUM_VALUES_DISPLAY,
        recommendations=report.recommendations,
        patterns=report.patterns,
    )
