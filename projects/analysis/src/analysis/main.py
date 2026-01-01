"""Main analysis module for type refinement discovery."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import create_engine, inspect

from .inference import TypeInferenceEngine
from .reporting import TEMPLATE_DIR, json_default
from .statistics import StatisticsCollector
from .types import AnalysisReport

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from sqlalchemy.engine import Engine

    from .types import TypeRecommendation

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


def validate_engine(engine: Engine) -> None:
    """Validate that the database has tables to analyze.

    Args:
        engine: SQLAlchemy engine connected to the database

    Raises:
        ValueError: If the database has no tables

    """
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if not table_names:
        msg = "Database has no tables to analyze"
        raise ValueError(msg)


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
    if suffix in {".sqlite", ".db", ".sqlite3"}:
        return create_engine(
            f"sqlite:///{database}",
            pool_size=5,  # Maintain 5 connections in pool
            max_overflow=10,  # Allow up to 10 additional connections
            pool_pre_ping=True,  # Verify connections before use
        )

    if suffix in {".mdb", ".accdb"}:
        abs_path = database.resolve()
        driver = "Microsoft Access Driver (*.mdb, *.accdb)"
        connection_string = f"DRIVER={{{driver}}};DBQ={abs_path}"
        return create_engine(
            f"access+pyodbc:///?odbc_connect={connection_string}",
            pool_size=5,  # Maintain 5 connections in pool
            max_overflow=10,  # Allow up to 10 additional connections
            pool_pre_ping=True,  # Verify connections before use
        )

    msg = f"Unsupported database extension: {suffix}"
    raise ValueError(msg)


def analyze_database(
    engine: Engine,
    *,
    confidence_threshold: float = 0.7,
) -> list[TypeRecommendation]:
    """Analyze database for type refinement opportunities.

    Args:
        engine: SQLAlchemy engine connected to the database
        confidence_threshold: Minimum confidence threshold for recommendations

    Returns:
        List of type recommendations

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

    return list(analyze_tables())


def report_to_json(report: AnalysisReport) -> str:
    """Convert AnalysisReport to JSON string.

    Args:
        report: AnalysisReport dataclass

    Returns:
        JSON string representation

    """
    report_dict = asdict(report)
    return json.dumps(report_dict, indent=2, default=json_default)


def generate_report(
    database_name: str,
    recommendations: list[TypeRecommendation],
    fmt: Literal["json", "markdown"],
) -> str:
    """Generate analysis report in requested format.

    Args:
        database_name: Name of the database
        recommendations: List of type recommendations
        fmt: Output format (json or markdown)

    Returns:
        Formatted report string

    """
    report = AnalysisReport(
        database=database_name,
        generated_at=datetime.now(UTC).isoformat(),
        recommendations=recommendations,
    )

    if fmt == "json":
        return report_to_json(report)
    return report_to_markdown(report)


def report_to_markdown(report: AnalysisReport) -> str:
    """Convert AnalysisReport to Markdown string.

    Args:
        report: AnalysisReport dataclass

    Returns:
        Markdown string representation

    """
    template = _JINJA_ENV.get_template("report.md")

    # Group recommendations by type and sort by confidence
    recommendations_by_type: defaultdict[str, list[TypeRecommendation]] = defaultdict(
        list,
    )
    for rec in report.recommendations:
        recommendations_by_type[rec.inferred_type.value].append(rec)

    for recs in recommendations_by_type.values():
        recs.sort(key=lambda r: r.confidence, reverse=True)

    return template.render(
        database_name=report.database,
        generated_at=report.generated_at,
        summary=report.summary,
        recommendations_by_type=recommendations_by_type,
        max_recommendations=MAX_RECOMMENDATIONS_DISPLAY,
        max_enum_values=MAX_ENUM_VALUES_DISPLAY,
        recommendations=report.recommendations,
    )
