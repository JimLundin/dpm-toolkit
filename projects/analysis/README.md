# Analysis Module

Meta-analysis tool for discovering type refinement opportunities in DPM databases.

## Purpose

This module analyzes **any database** supported by SQLAlchemy (Access, SQLite, PostgreSQL, etc.) to identify columns that could benefit from more specific type casting. It's a development tool that informs manual improvements to `projects/migrate/src/migrate/type_registry.py`.

**Flexible Analysis:** You can analyze Access databases before migration, SQLite databases after migration, or any other database to discover type refinement opportunities.

## Usage

```bash
# Analyze an Access database directly (Windows only)
dpm-toolkit analyze database.accdb

# Analyze a SQLite database (cross-platform)
dpm-toolkit analyze database.sqlite

# Generate markdown report
dpm-toolkit analyze database.sqlite --format markdown --output report.md

# Adjust confidence threshold (default: 0.7)
dpm-toolkit analyze database.sqlite --confidence 0.9
```

## Workflow Integration

See [WORKFLOW.md](./WORKFLOW.md) for detailed integration with your existing pipeline.

**Quick workflow (analyze before migration):**
1. Analyze: `dpm-toolkit analyze db.accdb --format markdown > analysis.md`
2. Review `analysis.md` for patterns in source data
3. Update `type_registry.py` with discovered patterns
4. Migrate with improved heuristics: `dpm-toolkit migrate db.accdb --target db.sqlite`

**Alternative workflow (analyze after migration):**
1. Migrate: `dpm-toolkit migrate db.accdb --target db.sqlite`
2. Analyze: `dpm-toolkit analyze db.sqlite --format markdown > analysis.md`
3. Review `analysis.md` for high-confidence recommendations
4. Update `type_registry.py` with discovered patterns
5. Re-migrate with improved heuristics

## What It Discovers

- **Enum candidates**: Low-cardinality VARCHAR columns
- **Naming patterns**: Suffix/prefix conventions (e.g., columns ending in `flag` are boolean)
- **Type mismatches**: Dates stored as VARCHAR, booleans as INTEGER
- **Pattern confidence**: Statistical backing for each recommendation

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

1. **Analyze** any database (Access, SQLite, etc.) to discover refinement opportunities
2. **Review** the generated report
3. **Manually update** `type_registry.py` with discovered patterns
4. **Re-migrate** databases with improved heuristics (if analyzing post-migration)
5. **Repeat** for new EBA releases

## Development

This is a standalone analysis tool, orthogonal to the migration pipeline. It works with any SQLAlchemy-supported database to inform type casting improvements.
