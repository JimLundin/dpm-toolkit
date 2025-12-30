"""Meta-analysis tool for discovering type refinement opportunities."""

from .inference import TypeInferenceEngine
from .main import (
    analyze_database,
    create_engine_for_database,
    generate_report,
    validate_engine,
)
from .pattern_mining import PatternMiner
from .statistics import StatisticsCollector
from .types import (
    AnalysisReport,
    ColumnStatistics,
    InferredType,
    NamePattern,
    ReportSummary,
    TypeRecommendation,
)

__all__ = [
    "AnalysisReport",
    "ColumnStatistics",
    "InferredType",
    "NamePattern",
    "PatternMiner",
    "ReportSummary",
    "StatisticsCollector",
    "TypeInferenceEngine",
    "TypeRecommendation",
    "analyze_database",
    "create_engine_for_database",
    "generate_report",
    "validate_engine",
]
