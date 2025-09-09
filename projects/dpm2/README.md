# DPM2

Generated Python models for EBA DPM 2.0 databases with full type safety and SQLAlchemy integration.

## Purpose

This package contains auto-generated, type-safe Python models for working with EBA DPM 2.0 databases. It provides:
- Fully typed SQLAlchemy ORM models
- Relationship mapping between tables
- IDE support with autocompletion and type checking
- Ready-to-use database models for analysis and reporting

## Installation

```bash
pip install dpm2
```

## Usage

```python
from sqlalchemy import create_engine, select
from dpm2 import DPM, TableVersionCell, Cell

# Connect to your SQLite database
engine = create_engine("sqlite:///path/to/dpm.sqlite")

# Type-safe queries with full IDE support
with engine.connect() as conn:
    stmt = select(TableVersionCell).where(
        TableVersionCell.IsActive == True
    )
    
    for row in conn.execute(stmt):
        print(f"Cell: {row.CellID} - {row.CellContent}")
```

## Features

- **Complete Type Safety** - All columns, relationships, and constraints are fully typed
- **IDE Integration** - Full autocompletion and error detection
- **SQLAlchemy 2.0+** - Uses modern SQLAlchemy with `Mapped` annotations
- **Relationship Navigation** - Foreign keys mapped to navigable Python objects
- **Documentation** - Every model includes generated documentation

## Generated Models

This package is automatically generated from the latest EBA DPM release and includes models for all DPM tables with proper relationships and constraints.

## Regeneration

Models are regenerated with each new EBA DPM release. Install the latest version to get updated schemas and data structures.