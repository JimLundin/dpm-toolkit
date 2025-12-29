"""Meta-analysis tool for discovering type refinement opportunities."""

from .inference import TypeInferenceEngine
from .main import analyze_database
from .pattern_mining import PatternMiner
from .reporting import ReportGenerator
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
    "ReportGenerator",
    "ReportSummary",
    "StatisticsCollector",
    "TypeInferenceEngine",
    "TypeRecommendation",
    "analyze_database",
]
