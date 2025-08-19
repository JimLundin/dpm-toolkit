# Compare

Comprehensive database comparison functionality for DPM Toolkit that identifies schema and data differences between SQLite databases.

## Usage

```bash
# Install the compare submodule
uv sync --extra compare

# Compare two databases using CLI
dpm-toolkit compare --source old.sqlite --target new.sqlite
```

## Architecture

The compare submodule consists of several focused components:

- **`inspector.py`**: Database introspection using SQLite system tables
- **`main.py`**: Core comparison logic and report generation
- **`types.py`**: Type definitions for comparison results
- **`templates/`**: HTML report templates with embedded CSS/JS

## What it does

### Schema Comparison

- Detects added, removed, and modified tables
- Identifies column changes including type, constraints, and default values
- Compares primary key definitions

### Data Comparison

- Finds added, removed, and modified rows
- Uses intelligent row matching via RowGUID or primary key columns
- Handles tables with different primary key structures

### Report Generation

- Produces structured JSON output for programmatic consumption
- Generates comprehensive HTML reports with interactive features
- Streams large datasets efficiently without loading everything into memory

## API Overview

### Core Functions

```python
from compare import compare_databases, render_report, comparisons_to_json

# Compare databases and get iterator of differences
comparisons = compare_databases(source_conn, target_conn)

# Generate HTML report
html_stream = render_report(comparisons)

# Convert to JSON
json_output = comparisons_to_json(comparisons)
```

### Database Inspector

```python
from compare.inspector import Inspector

inspector = Inspector(db_connection)
tables = list(inspector.tables())           # Get all table names
columns = list(inspector.cols("table"))     # Get column metadata
primary_keys = list(inspector.pks("table")) # Get primary key columns
rows = list(inspector.rows("table"))        # Get all table data
```

## Row Matching Strategy

The comparison uses a sophisticated row matching approach:

1. **RowGUID preferred**: If a `RowGUID` column exists, it's used as the unique identifier
2. **Primary key fallback**: Uses primary key columns to identify rows
3. **Full row fallback**: If no primary key exists, uses all column values

This handles databases with different primary key structures gracefully.

## Output Format

Results are structured as `Comparison` objects containing:

- Table name
- Column differences (added/removed/modified)
- Row differences (added/removed/modified)

Each difference is represented as a `Mod` object with optional `old` and `new` values.
