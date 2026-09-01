# DPM2

Generated Python models for EBA DPM 2.0 databases with full type safety and SQLAlchemy integration.

## Purpose

This package contains auto-generated, type-safe Python models for working with EBA DPM 2.0 databases. It provides:
- Fully typed SQLAlchemy ORM models
- Relationship mapping between tables
- Bundled SQLite database ready to query
- IDE support with autocompletion and type checking

## Installation

```bash
pip install dpm2
```

## Usage

```python
from sqlalchemy import select
from dpm2 import get_db, models

# Get an in-memory engine backed by the bundled database (default)
engine = get_db()

# Type-safe queries with full IDE support
with engine.connect() as conn:
    stmt = select(models.ConceptClass)

    for row in conn.execute(stmt):
        print(row)
```

### First use

The wheel ships the database LZMA-compressed to stay within PyPI's per-file
size limit. The first call to `get_db()` decompresses it once (a few seconds,
~300 MB) into a per-version cache directory; later calls reuse that copy.

The cache lives under `%LOCALAPPDATA%\dpm2\cache` on Windows and
`$XDG_CACHE_HOME/dpm2` (default `~/.cache/dpm2`) elsewhere. Set `DPM2_CACHE_DIR`
to put it somewhere else:

```bash
export DPM2_CACHE_DIR=/data/dpm2-cache
```

```python
from dpm2 import cached_db_path, ensure_local_db

cached_db_path()    # where the decompressed database lives
ensure_local_db()   # decompress now (e.g. to warm the cache in a Docker build)
```

### Engine options

```python
from dpm2 import get_db, disk_engine

# In-memory copy (default) - fast, safe for temporary work
engine = get_db()

# Read-only file-backed engine - lower memory usage
engine = get_db(in_memory=False)

# Custom path with disk_engine
from pathlib import Path
engine = disk_engine(Path("my_local_copy.sqlite"))
```

## Features

- **Complete Type Safety** - All columns, relationships, and constraints are fully typed
- **IDE Integration** - Full autocompletion and error detection
- **SQLAlchemy 2.0+** - Uses modern SQLAlchemy with `Mapped` annotations
- **Relationship Navigation** - Foreign keys mapped to navigable Python objects
- **Bundled Database** - Ships with a ready-to-query SQLite database (decompressed once on first use)

## Generated Models

This package is automatically generated from the latest EBA DPM release and includes models for all DPM tables with proper relationships and constraints.

## Regeneration

Models are regenerated with each new EBA DPM release. Install the latest version to get updated schemas and data structures.