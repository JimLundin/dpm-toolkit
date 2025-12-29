# Workflow Integration

## What to Analyze

Analyze **any database** supported by SQLAlchemy:
- **Access databases** (.accdb, .mdb) - analyze source data before migration (Windows only)
- **SQLite databases** (.sqlite, .db) - analyze migrated output
- **Any database URL** - PostgreSQL, MySQL, etc.

## Basic Usage

```bash
# Analyze Access database directly (Windows)
dpm-toolkit analyze db.accdb --format markdown > analysis.md

# Analyze migrated SQLite database
dpm-toolkit migrate db.accdb --target db.sqlite
dpm-toolkit analyze db.sqlite --format markdown > analysis.md

# Review analysis.md for high-confidence patterns
# Update projects/migrate/src/migrate/type_registry.py
# Re-migrate with improved heuristics
```

## What It Finds

- **Enum candidates**: Low-cardinality VARCHAR columns
- **Naming patterns**: Suffixes/prefixes correlated with types (e.g., `*flag` → boolean)
- **Type mismatches**: Dates stored as text, booleans as integers

## Applying Recommendations

Review the generated report and manually update `type_registry.py`:

```python
# Example: Add discovered pattern
def column_type(col: Column) -> DataType | None:
    col_name = col.name.lower()

    if col_name.endswith("flag"):  # Discovered from analysis
        return BOOLEAN

    # ... existing patterns ...
```

Then re-migrate to validate improvements.
