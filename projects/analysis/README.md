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
- **Boolean patterns**: Columns with binary values matching boolean patterns
- **UUID patterns**: VARCHAR columns containing UUID values
- **Date/DateTime patterns**: Columns containing date or datetime values

## Output

Reports include:
- Type recommendations with confidence scores
- Statistical evidence for each recommendation
- Enum value sets for low-cardinality columns
