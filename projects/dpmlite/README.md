# DPM Lite

Lightweight subset of EBA DPM 2.0 focused on report structure, table layout, and validation rules.

## Purpose

This package is derived from the full [dpm2](https://pypi.org/project/dpm2/) package by filtering the database to only the tables needed for building and validating reports. It provides:
- Fully typed SQLAlchemy ORM models for the included tables
- Relationship mapping between tables
- Bundled SQLite database ready to query
- Significantly smaller install size than the full dpm2 package

## Installation

```bash
pip install dpmlite
```

## Usage

```python
from sqlalchemy import select
from dpmlite import get_db, models

# Get an in-memory engine backed by the bundled database (default)
engine = get_db()

# Type-safe queries with full IDE support
with engine.connect() as conn:
    stmt = select(models.TableVersion)

    for row in conn.execute(stmt):
        print(row)
```

### Engine options

```python
from dpmlite import get_db, disk_engine

# In-memory copy (default) - fast, safe for temporary work
engine = get_db()

# Read-only file-backed engine - lower memory usage
engine = get_db(in_memory=False)

# Custom path with disk_engine
from pathlib import Path
engine = disk_engine(Path("my_local_copy.sqlite"))
```

## Included Table Domains

DPM Lite includes tables from three domains:

1. **Report structure** — templates, modules, and how reports are organised
2. **Table layout** — the row/column structure of each reporting table
3. **Validation rules** — the business rules applied to reported data

Plus shared reference tables that these domains depend on.

See `dpmlite.tables.INCLUDED_TABLES` for the exact set of tables included.

## Relationship to dpm2

DPM Lite is built from the same EBA DPM source as dpm2, using the committed `dpm2/models.py` as the source of truth for available tables. The build pipeline filters the full database to the dpmlite allowlist and regenerates models for the subset.

## Regeneration

Models are regenerated with each new EBA DPM release alongside dpm2. Install the latest version to get updated schemas and data structures.
