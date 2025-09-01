# Schema

Python model generation from converted SQLite databases.

## Purpose

The schema module provides:
- Automatic Python model generation from SQLite databases
- Type-safe SQLAlchemy model creation
- Relationship mapping and foreign key detection
- Code generation with full type annotations

## Key Functions

- `sqlite_read_only()` - Create read-only SQLAlchemy engine for SQLite database
- `sqlite_to_sqlalchemy_schema()` - Generate SQLAlchemy schema from migrated SQLite database

## Usage

```python
from pathlib import Path
from schema.main import sqlite_read_only, sqlite_to_sqlalchemy_schema

# Create read-only database connection
database_path = Path("/path/to/database.sqlite")
engine = sqlite_read_only(database_path)

# Generate Python models from SQLite database
schema_code = sqlite_to_sqlalchemy_schema(engine)

# Write to file
with open("/path/to/models.py", "w") as f:
    f.write(schema_code)
```

## Generated Features

- **Type Annotations** - Full type hints for all columns and relationships
- **SQLAlchemy Models** - Ready-to-use ORM models
- **Relationship Mapping** - Automatic foreign key relationships
- **Enum Types** - Constrained values as Python Literal types
- **Documentation** - Auto-generated docstrings

## Output Example

```python
class TableVersionCell(DPM):
    """Auto-generated model for the TableVersionCell table."""
    __tablename__ = "TableVersionCell"

    cell_id: Mapped[str] = mapped_column("CellID", primary_key=True)
    cell_content: Mapped[str | None] = mapped_column("CellContent")
    is_active: Mapped[bool] = mapped_column("IsActive")
    
    # Auto-generated relationships
    cell: Mapped[Cell] = relationship(foreign_keys=cell_id)
```

This is an internal DPM Toolkit component - generated models are distributed via the `dpm2` package.