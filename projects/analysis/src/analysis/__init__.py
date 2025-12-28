"""Meta-analysis tool for discovering type refinement opportunities."""

from .inference import TypeInferenceEngine
from .main import analyze_database, compare_databases
from .pattern_mining import PatternMiner
from .reporting import AnalysisReport
from .statistics import StatisticsCollector
from .types import (
    ColumnStatistics,
    InferredType,
    NamePattern,
    TypeRecommendation,
)

__all__ = [
    "AnalysisReport",
    "ColumnStatistics",
    "InferredType",
    "NamePattern",
    "PatternMiner",
    "StatisticsCollector",
    "TypeInferenceEngine",
    "TypeRecommendation",
    "analyze_database",
    "compare_databases",
]
