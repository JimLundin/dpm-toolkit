# Schema

Python model generation from converted SQLite databases.

## Purpose

The schema module provides:
- Automatic Python model generation from SQLite databases
- Type-safe SQLAlchemy model creation
- Relationship mapping and foreign key detection
- Code generation with full type annotations

## Key Functions

- `generate_schema()` - Generate Python models from SQLite database schema

## Usage

```python
from schema import generate_schema

# Generate Python models from SQLite database
generate_schema(
    database_path="/path/to/database.sqlite",
    output_path="/path/to/models.py"
)
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
    """Auto-generated model for TableVersionCell table."""
    __tablename__ = "TableVersionCell"
    
    CellID: Mapped[str] = mapped_column(primary_key=True)
    CellContent: Mapped[str | None]
    IsActive: Mapped[bool]
    
    # Auto-generated relationships
    Cell: Mapped[Cell] = relationship(foreign_keys=[CellID])
```

This is an internal DPM Toolkit component - generated models are distributed via the `dpm2` package.