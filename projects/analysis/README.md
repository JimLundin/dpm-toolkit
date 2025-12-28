# Analysis Module

Meta-analysis tool for discovering type refinement opportunities in DPM databases.

## Purpose

This module analyzes SQLite databases to identify columns that could benefit from more specific type casting. It's designed to inform improvements to the migration pipeline's heuristics in `type_registry.py`, **not** to modify the migration logic itself.

## Features

- **Statistics Collection**: Analyzes column data to collect value distributions, cardinality, and patterns
- **Type Inference**: Identifies columns that should be enums, booleans, dates, UUIDs, etc.
- **Pattern Mining**: Discovers naming conventions (suffixes, prefixes) correlated with types
- **Report Generation**: Produces actionable reports in JSON and Markdown formats

## Usage

Via the main CLI:

```bash
# Analyze a single database
dpm-toolkit analyze database.sqlite

# Generate markdown report
dpm-toolkit analyze database.sqlite --format markdown

# Custom output path
dpm-toolkit analyze database.sqlite --output my-report.json

# Adjust confidence threshold
dpm-toolkit analyze database.sqlite --confidence 0.8
```

## Architecture

```
analyze_database()
    ↓
StatisticsCollector → Collects column statistics from database
    ↓
TypeInferenceEngine → Infers better types from statistics
    ↓
PatternMiner → Discovers naming conventions
    ↓
AnalysisReport → Generates JSON/Markdown reports
```

## Type Inference Rules

### Enum Detection
- Cardinality ≤ 50 unique values
- Cardinality ratio ≤ 1% (unique/total)
- At least 2 distinct values

### Boolean Detection
- Exactly 2 unique values
- Values match common boolean pairs: (0, 1), (true, false), (yes, no), etc.

### Date/DateTime Detection
- Pattern matching: ≥95% of values match date/datetime formats
- Supports: ISO, DD/MM/YYYY, MM/DD/YYYY, DD-MM-YYYY, etc.

### UUID Detection
- Pattern matching: ≥95% of values match UUID format

## Output Formats

### JSON
Machine-readable format with:
- Summary statistics
- All recommendations with evidence
- Discovered patterns

### Markdown
Human-readable format with:
- Summary tables
- Suggested code changes for `type_registry.py`
- Top candidates by type
- Pattern discovery results

## Integration with Workflow

1. **Analyze** a migrated database to discover refinement opportunities
2. **Review** the generated report
3. **Manually update** `type_registry.py` with discovered patterns
4. **Re-migrate** databases with improved heuristics
5. **Repeat** for new EBA releases

## Development

This is a standalone analysis tool, orthogonal to the migration pipeline. It reads databases post-migration to inform future improvements.
