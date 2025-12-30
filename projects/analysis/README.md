# Analysis

Meta-analysis tool for discovering type refinement opportunities in DPM databases.

## Usage

```bash
# Analyze database (SQLite, Access, etc.)
dpm-toolkit analyze database.sqlite

# Generate markdown report
dpm-toolkit analyze database.sqlite --fmt markdown --output report.md

# Adjust confidence threshold (default: 0.7)
dpm-toolkit analyze database.sqlite --confidence 0.9
```

## What It Discovers

- **Enum candidates**: Low-cardinality VARCHAR columns
- **Boolean patterns**: Columns with binary values or boolean-like naming
- **UUID patterns**: VARCHAR columns containing UUID values
- **Naming patterns**: Suffix/prefix conventions correlated with specific types

## Output

Reports include:
- Type recommendations with confidence scores
- Pattern analysis (e.g., columns ending in `_flag` → boolean)
- Statistical backing for each recommendation
