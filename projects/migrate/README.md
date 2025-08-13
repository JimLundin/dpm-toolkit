# Migrate

Database migration tools for converting Microsoft Access databases to SQLite with enhanced typing and relationships.

## Purpose

The migrate module provides:
- Access database to SQLite conversion
- Type enhancement and refinement
- Relationship mapping and foreign key detection
- Data cleaning and optimization

## Platform Requirements

**Windows Only** - Requires Microsoft Access ODBC drivers and `sqlalchemy-access` package.

## Key Functions

- `migrate_to_sqlite()` - Convert Access database to SQLite with type improvements

## Usage

```python
from migrate import migrate_to_sqlite

# Convert Access database to SQLite
migrate_to_sqlite(
    source_path="/path/to/database.accdb",
    target_path="/path/to/output.sqlite"
)
```

## Features

- **Smart Type Detection** - Infers better types from column names and data
- **Relationship Mapping** - Detects and preserves foreign key relationships  
- **Data Cleaning** - Handles Access-specific data formats and constraints
- **Performance Optimization** - Optimizes SQLite indexes and constraints

## Dependencies

- `sqlalchemy` - Database toolkit
- `sqlalchemy-access` - Access database support (Windows only)

This is an internal OpenDPM component - use via `opendpm migrate` command instead of directly.