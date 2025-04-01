# OpenDPM

Convert EBA DPM 2.0 (Data Point Model) databases from AccessDB to SQLite.

## Disclaimer

This is an unofficial tool and is not affiliated with or endorsed by the European Banking Authority (EBA). The original AccessDB source is available at the [EBA DPM Website](https://www.eba.europa.eu/risk-and-data-analysis/reporting-frameworks/dpm-data-dictionary).

## Overview

This project provides pre-converted SQLite versions of the EBA DPM 2.0 databases accessible across all platforms. It also generates SQLAlchemy models that document the database structure.

### Why This Project?

The current DPM 2.0 releases from EBA are available only in AccessDB format, with limited support outside Windows. Additionally, DPM releases are provided in "incremental" format, with each release self-contained. This project addresses these issues by:
- Converting databases to SQLite
- Combining releases into a single file
- Providing HTTP access to the data
- Automating conversion through GitHub Actions
- Publishing converted databases as releases
- Generating SQLAlchemy models of the database structure

### Database Compatibility

The converted SQLite databases maintain compatibility with the original Access database structure, preserving relationships and constraints. Due to cyclic dependencies, referential integrity may not be fully preserved, and Foreign Key constraints are not enforced.

### SQLAlchemy Models

The project generates SQLAlchemy ORM models that document the database structure. These models offer:

- **Database Documentation**: Details of tables, columns, relationships, and data types
- **Type Safety**: Type-annotated classes compatible with IDEs and type checkers
- **Code Completion**: Autocompletion for database operations in Python code
- **Relationship Navigation**: Direct access to related tables through mapped relationships

This provides development tooling not available when working with the original Access database.

## Getting Started

### Option 1: Download Pre-converted Databases

Download converted databases and models from our [Releases page](https://github.com/JimLundin/opendpm/releases).

You can access the latest release via:
- URL for scripts:
```
https://github.com/JimLundin/opendpm/releases/latest/download/dpm-sqlite.zip
```
- Or direct link: [Download Latest DPM SQLite Database](https://github.com/JimLundin/opendpm/releases/latest/download/dpm-sqlite.zip)

### Option 2: Run the Conversion Yourself

- Online:
   1. Fork this repository
   2. Enable GitHub Actions in your fork
   3. Run the conversion workflow
   4. Access the converted databases and models in the workflow artifacts

- Offline:
   1. Clone this repository
   2. Install dependencies using `pip install`
   3. Run conversion using the `opendpm` CLI tool
   4. Find the converted databases and models in the output directory

## Advanced Database Features

### Database Refinement Process

The conversion process refines the database to improve accessibility and consistency while preserving the original structure:

1. **Type Refinement**: Enhances column data types based on:
   - Column naming conventions (columns ending with "GUID", "Date", or starting with "is"/"has")
   - Predefined mappings for special cases
   - Type generalization for consistent representation

2. **Constraint Enhancement**:
   - Detects nullable columns from data analysis
   - Identifies and applies enum types
   - Establishes missing foreign key relationships
   - Optimizes primary key indexes for SQLite

3. **Value Casting**: Transforms data to appropriate Python types:
   - Converts date strings to date objects
   - Normalizes boolean values (from Access's -1/0 to True/False)
   - Applies custom transformations as needed

### SQLAlchemy Model Generation

The project generates SQLAlchemy ORM models with these features:

1. **Typed Data Access**: 
   - Type-annotated models with proper Python types
   - Support for Literal types for enums
   - Optional types for nullable columns

2. **Relationship Mapping**:
   - Automatic detection of table relationships
   - Foreign key relationships mapped to SQLAlchemy relationship objects
   - Column-based naming conventions for relationships

3. **Code Quality**:
   - Auto-generated documentation for models and tables
   - PEP-8 compliant Python code

4. **Model Structure**:
   - Tables with primary keys as SQLAlchemy ORM classes
   - Tables without primary keys as SQLAlchemy Tables
   - Support for composite primary keys and complex relationships

Example of a generated model:

```python
class TableVersionCell(DPM):
    """Auto-generated model for the TableVersionCell table."""
    __tablename__ = "TableVersionCell"

    CellID: Mapped[str] = mapped_column(primary_key=True)
    TableVersionCellID: Mapped[str]
    CellContent: Mapped[str | None]
    # ... other columns ...

    # Automatically generated relationships
    Cell: Mapped[Cell] = relationship(foreign_keys=[CellID])
    TableVersionHeader: Mapped[TableVersionHeader] = relationship(foreign_keys=[TableVersionHeaderID])
```

These models provide type-safe database interaction with IDE integration and code completion.

## Caveats

- Only current DPM release data is extracted, not data from previous releases
- Original database queries are not preserved
- Local conversion requires Microsoft Access ODBC driver

## Development

To run the conversion locally:

1. Clone the repository
2. Install Python 3.12+
3. Install Microsoft Access ODBC driver
4. Install package in development mode: `pip install -e .`
5. Run the conversion:
   ```
   opendpm convert <source_dir> <target_dir>
   ```

## Contributing

Contributions are welcome. Please submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
