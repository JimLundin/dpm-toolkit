# DMP Toolkit

Open-source tools and models for working with EBA DPM 2.0 (Data Point Model) databases.

**📚 Documentation:** [Architecture](#architecture-overview) | [CLI Reference](#cli-reference) | [Contributing](CONTRIBUTING.md) | [Projects](#project-components)

## Disclaimer

This is an unofficial tool and is not affiliated with or endorsed by the European Banking Authority (EBA). The original AccessDB source is available at the [EBA DPM Website](https://www.eba.europa.eu/risk-and-data-analysis/reporting-frameworks/dpm-data-dictionary).

## What is DPM Toolkit?

DPM Toolkit makes EBA DPM 2.0 databases accessible across all platforms by converting Windows-only Access databases to SQLite and generating type-safe Python models.

### Key Benefits

- **Cross-Platform Access**: SQLite databases work on Windows, macOS, and Linux
- **Type-Safe Development**: Auto-generated SQLAlchemy models with IDE support
- **Automated Updates**: CI/CD pipeline ensures latest versions are always available
- **Multiple Options**: Download pre-built artifacts or convert databases yourself
- **Zero Setup**: Ready-to-use databases and Python models

### Why Use DPM Toolkit?

**For Data Analysts**: Skip the hassle of Windows-only Access databases. Get clean SQLite files that work everywhere.

**For Python Developers**: Type-annotated models with relationship mapping, autocompletion, and documentation.

**For Organizations**: Automated pipeline keeps databases current with EBA releases.

**For Compliance Teams**: Maintains original database structure and relationships while improving accessibility.

## Quick Start

### Install DPM Toolkit

```bash
# Basic installation (recommended for most users)
pip install dpm-toolkit

# With optional extras for specific functionality
pip install dpm-toolkit[scrape]    # Web scraping capabilities
pip install dpm-toolkit[migrate]   # Database migration (Windows only)
pip install dpm-toolkit[schema]    # Python model generation
```

### Download Latest Database

```bash
# List available versions
dpm-toolkit list

# Download latest release (SQLite)
dpm-toolkit download --version release --type converted

# Download specific version
dpm-toolkit download --version "3.2" --type converted
```

### Use in Python

First install the generated models package:

```bash
pip install dpm2
```

Then use the bundled database and models:

```python
from dpm2 import get_db
from dpm2.models import TableVersionCell, Cell

# Get database connection (bundled SQLite database)
engine = get_db()

# Type-safe database operations with IDE support
with engine.connect() as conn:
    # Your code here with full type checking and autocompletion
    pass
```

## Platform-Specific Options

### All Platforms (Recommended)

Download pre-converted SQLite databases and Python models:

```bash
# Download from CLI (recommended)
dpm-toolkit download --version release --type converted

# Or download directly from GitHub releases
# https://github.com/JimLundin/dpm-toolkit/releases/latest/download/dpm-sqlite.zip
```

### Windows Only - Self Conversion

⚠️ **Windows Requirement**: Database conversion requires Microsoft Access ODBC driver and is only supported on Windows due to `sqlalchemy-access` and `pyodbc` dependencies.

```bash
# Install with conversion support (Windows only)
pip install dpm-toolkit[migrate]

# Convert your own Access databases
dpm-toolkit migrate --source /path/to/access/database.accdb --target /path/to/output.sqlite
```

### Non-Windows Users

- **Recommended**: Use pre-built artifacts from releases or CLI download
- **Alternative**: Set up Windows VM if self-conversion is absolutely required
- **Not Supported**: Direct conversion on macOS/Linux

## CLI Reference

### Core Commands

```bash
# List available database versions
dpm-toolkit list [--version VERSION] [--json|--yaml|--table]

# Download databases and models
dpm-toolkit download [--version VERSION] [--type TYPE] [--target DIRECTORY]
                 [--extract|--no-extract] [--overwrite]

# Find new versions (maintenance)
dpm-toolkit update [--json|--yaml|--table]

# Convert Access to SQLite (Windows only)
dpm-toolkit migrate --source SOURCE --target TARGET [--overwrite]

# Generate Python models from SQLite
dpm-toolkit schema --source SOURCE [--target TARGET]
```

### Version Selection

- `--version release` - Latest stable release (recommended, default)
- `--version latest` - Most recent version (including prereleases)
- `--version "X.Y"` - Specific version (e.g., "3.2")

### Download Types

- `--type converted` - SQLite database + Python models (default, recommended)
- `--type original` - Original EBA Access database
- `--type archive` - Processed Access database

### Examples

```bash
# Download latest stable release
dpm-toolkit download --version release

# Download specific version to custom directory
dpm-toolkit download --version "3.2" --target ./dpm-data

# List all versions in JSON format
dpm-toolkit list --json

# Convert local Access database (Windows only)
dpm-toolkit migrate --source ./database.accdb --target ./output.sqlite

# Generate Python models from SQLite database
dpm-toolkit schema --source ./output.sqlite --target ./models.py
```

## Using the Generated Models

### Database Access

```python
from sqlalchemy import select
from dpm2 import get_db
from dpm2.models import TableVersionCell, Cell

# Get bundled database connection (no setup required)
engine = get_db()

# Type-safe queries with IDE support
with engine.connect() as conn:
    # Query with autocompletion and type checking
    stmt = select(TableVersionCell).where(TableVersionCell.cell_content.isnot(None))
    result = conn.execute(stmt)

    for row in result:
        print(f"Cell ID: {row.cell_id}, Content: {row.cell_content}")

# Alternative: Use in-memory database for better performance
engine = get_db(in_memory=True)
```

### Model Features

The generated SQLAlchemy models provide:

- **Type Annotations**: Full type hints for all columns and relationships
- **Automatic Relationships**: Foreign key relationships mapped to Python objects
- **Enum Types**: Constrained values represented as Python Literal types
- **Nullable Detection**: Optional types for columns that can be NULL
- **IDE Integration**: Full autocompletion and type checking support

### Example Generated Model

```python
# Example from dpm2.models
class TableVersionCell(DPM):
    """Auto-generated model for the TableVersionCell table."""
    __tablename__ = "TableVersionCell"

    cell_id: Mapped[str] = mapped_column(primary_key=True)
    table_version_cell_id: Mapped[str]
    cell_content: Mapped[str | None]  # Nullable column
    is_active: Mapped[bool]           # Boolean type
    created_date: Mapped[date]        # Date type

    # Automatically generated relationships
    cell: Mapped[Cell] = relationship(foreign_keys=[cell_id])
    table_version_header: Mapped[TableVersionHeader] = relationship(
        foreign_keys=[table_version_cell_id]
    )
```

## Database Conversion Process

The conversion process enhances the original Access database structure:

### 1. Type Refinement

- **Smart Type Detection**: Infers better types from column names and data
- **Date Conversion**: Converts Access date strings to Python date objects
- **Boolean Normalization**: Transforms Access -1/0 to Python True/False
- **GUID Recognition**: Identifies UUID columns by naming patterns

### 2. Constraint Enhancement

- **Nullable Analysis**: Detects which columns can be NULL from actual data
- **Enum Detection**: Identifies constrained value sets and creates Literal types
- **Relationship Mapping**: Establishes foreign key relationships
- **Primary Key Optimization**: Optimizes indexes for SQLite performance

### 3. Model Generation

- **Type-Safe Classes**: Creates fully annotated SQLAlchemy models
- **Relationship Objects**: Maps foreign keys to navigable Python relationships
- **Documentation**: Auto-generates docstrings for all models and tables
- **Code Quality**: Produces PEP-8 compliant, linted Python code

## Architecture Overview

DPM Toolkit is built as a modular workspace with specialized components:

### Project Components

- **[`dpm-toolkit`](src/dpm-toolkit/)**: Central CLI that coordinates all functionality
- **[`archive`](projects/archive/)**: Version management, downloads, and release tracking
- **[`migrate`](projects/migrate/)**: Access-to-SQLite conversion engine (Windows only)
- **[`scrape`](projects/scrape/)**: Automated discovery of new EBA releases
- **[`schema`](projects/schema/)**: Python model generation from SQLite databases
- **[`dpm2`](projects/dpm2/)**: Generated Python models package

### Automated Pipeline

1. **Discovery**: GitHub Actions automatically detect new EBA releases
2. **Conversion**: Windows runners convert Access databases to SQLite
3. **Model Generation**: Creates type-safe SQLAlchemy models
4. **Publishing**: Releases artifacts as GitHub releases
5. **Distribution**: Makes databases available via CLI and direct download

## Important Notes

### Platform Limitations

- **Conversion**: Only supported on Windows due to Microsoft Access ODBC driver requirements
- **SQLAlchemy-Access**: Depends on `pyodbc` and Win32 APIs
- **Recommended**: Use pre-built artifacts for non-Windows platforms

### Database Compatibility

- **Structure Preservation**: Maintains original Access database schema
- **Relationship Mapping**: Preserves table relationships where possible
- **Constraint Limitations**: Some referential integrity constraints may not be fully enforced due to cyclic dependencies
- **Data Currency**: Only current DPM release data is included, not historical versions

---

## Developer Guide

### Development Setup

```bash
# Clone the repository
git clone https://github.com/JimLundin/dpm-toolkit.git
cd dpm-toolkit

# Install UV package manager
pip install uv

# Install all dependencies
uv sync

# Install in development mode
uv pip install -e .
```

### Project Structure

DPM Toolkit uses a UV workspace with multiple subprojects:

```
dpm-toolkit/
├── src/dpm-toolkit/           # Main CLI package
├── projects/              # Workspace subprojects
│   ├── archive/          # Version management & downloads
│   ├── migrate/          # Access-to-SQLite conversion
│   ├── scrape/           # Web scraping for new versions
│   ├── schema/           # Python model generation
│   └── dpm2/             # Generated Python models package
├── .github/workflows/    # CI/CD automation
└── pyproject.toml        # Workspace configuration
```

### Working with Subprojects

Each subproject is independently installable:

```bash
# Install specific subprojects
uv pip install -e projects/archive
uv pip install -e projects/migrate  # Windows only
uv pip install -e projects/scrape
uv pip install -e projects/schema
```

### Code Quality

The project uses strict code quality tools:

```bash
# Run linting and formatting
ruff check --fix
ruff format

# Type checking
mypy src/
pyright src/
```

### Testing

```bash
# Run tests (when available)
uv run pytest
```

### Requirements

- **Python**: 3.13+
- **Package Manager**: UV (recommended) or pip
- **Platform**: Windows required for conversion functionality
- **Dependencies**: Microsoft Access ODBC driver (for conversion)

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Ensure code quality checks pass
5. Submit a Pull Request

Contributions are welcome! Please ensure all code follows the project's quality standards and includes appropriate tests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
