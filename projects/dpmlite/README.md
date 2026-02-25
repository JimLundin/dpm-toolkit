# DPM Lite

Lightweight subset of EBA DPM 2.0 focused on report structure, table layout, and validation rules.

## Purpose

This package is derived from the full [dpm2](https://pypi.org/project/dpm2/) package at build time. It uses dpm2's typed SQLAlchemy models to query the full database and writes the results into dpmlite's own hand-written models. Both sides are fully type-checked — if dpm2's schema changes in a way that breaks a query, you get a real error.

At runtime dpmlite is standalone — it ships its own bundled SQLite database and models with no dependency on dpm2.

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

## Architecture

### Models (hand-written)

`dpmlite/models.py` defines the simplified SQLAlchemy schema. These models are committed source code, not generated output. They represent an intentional subset of DPM 2.0.

### Build script

`build_db.py` reads from `dpm2.models` (typed) and writes into `dpmlite.models` (typed). This gives full type safety on both sides:

```python
from dpm2.models import Template as DpmTemplate  # typed reads
from dpmlite.models import Template               # typed writes

for row in source.execute(select(DpmTemplate)):
    dest.add(Template(template_id=row.template_id, name=row.name))
```

### Included domains

1. **Report structure** — templates, modules, and how reports are organised
2. **Table layout** — the row/column structure of each reporting table
3. **Validation rules** — the business rules applied to reported data

## Regeneration

The bundled database is rebuilt with each release by running `build_db.py` against the latest dpm2. Install the latest version to get updated data.
