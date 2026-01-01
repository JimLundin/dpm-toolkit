"""Meta-analysis tool for discovering type refinement opportunities."""

from .inference import TypeInferenceEngine
from .main import (
    analyze_database,
    create_engine_for_database,
    generate_report,
    validate_engine,
)
from .statistics import StatisticsCollector
from .types import (
    AnalysisReport,
    ColumnStatistics,
    InferredType,
    ReportSummary,
    TypeRecommendation,
)

__all__ = [
    "AnalysisReport",
    "ColumnStatistics",
    "InferredType",
    "ReportSummary",
    "StatisticsCollector",
    "TypeInferenceEngine",
    "TypeRecommendation",
    "analyze_database",
    "create_engine_for_database",
    "generate_report",
    "validate_engine",
]
