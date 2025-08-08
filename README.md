# OpenDPM

Open-source tools and models for working with EBA DPM 2.0 (Data Point Model) databases.

## Disclaimer

This is an unofficial tool and is not affiliated with or endorsed by the European Banking Authority (EBA). The original AccessDB source is available at the [EBA DPM Website](https://www.eba.europa.eu/risk-and-data-analysis/reporting-frameworks/dpm-data-dictionary).

## What is OpenDPM?

OpenDPM makes EBA DPM 2.0 databases accessible across all platforms by converting Windows-only Access databases to SQLite and generating type-safe Python models.

### Key Benefits

- **Cross-Platform Access**: SQLite databases work on Windows, macOS, and Linux
- **Type-Safe Development**: Auto-generated SQLAlchemy models with IDE support
- **Automated Updates**: CI/CD pipeline ensures latest versions are always available
- **Multiple Options**: Download pre-built artifacts or convert databases yourself
- **Zero Setup**: Ready-to-use databases and Python models

### Why Use OpenDPM?

**For Data Analysts**: Skip the hassle of Windows-only Access databases. Get clean SQLite files that work everywhere.

**For Python Developers**: Type-annotated models with relationship mapping, autocompletion, and documentation.

**For Organizations**: Automated pipeline keeps databases current with EBA releases.

**For Compliance Teams**: Maintains original database structure and relationships while improving accessibility.

## Quick Start

### Install OpenDPM

```bash
pip install opendpm
```

### Download Latest Database

```bash
# List available versions
opendpm list

# Download latest release (SQLite + Python models)
opendpm download --release --converted

# Download specific version
opendpm download --version "3.2" --converted
```

### Use in Python

```python
from sqlalchemy import create_engine
from dpm import DPM, TableVersionCell

# Connect to downloaded database
engine = create_engine("sqlite:///dpm.sqlite")

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
opendpm download --release --converted

# Or download directly from GitHub releases
# https://github.com/JimLundin/opendpm/releases/latest/download/dpm-sqlite.zip
```

### Windows Only - Self Conversion

⚠️ **Windows Requirement**: Database conversion requires Microsoft Access ODBC driver and is only supported on Windows due to `sqlalchemy-access` and `pyodbc` dependencies.

```bash
# Install with conversion support (Windows only)
pip install opendpm[convert]

# Convert your own Access databases
opendpm migrate --source /path/to/access/files --target /path/to/output
```

### Non-Windows Users

- **Recommended**: Use pre-built artifacts from releases or CLI download
- **Alternative**: Set up Windows VM if self-conversion is absolutely required
- **Not Supported**: Direct conversion on macOS/Linux

## CLI Reference

### Core Commands

```bash
# List available database versions
opendpm list [--release|--latest|--version VERSION] [--json]

# Download databases and models
opendpm download [--release|--latest|--version VERSION] 
                 [--original|--archive|--converted] 
                 [--target DIRECTORY]

# Find new versions (maintenance)
opendpm scrape [--json]

# Convert Access to SQLite (Windows only)
opendpm migrate --source SOURCE --target TARGET
```

### Version Selection

- `--release` - Latest stable release (recommended)
- `--latest` - Most recent version (including prereleases)
- `--version "X.Y"` - Specific version (e.g., "3.2")

### Download Types

- `--converted` - SQLite database + Python models (default, recommended)
- `--original` - Original EBA Access database
- `--archive` - Processed Access database

### Examples

```bash
# Download latest stable release
opendpm download --release

# Download specific version to custom directory
opendpm download --version "3.2" --target ./dpm-data

# List all versions in JSON format
opendpm list --json

# Convert local Access database (Windows only)
opendpm migrate --source ./access-files --target ./sqlite-output
```

## Using the Generated Models

### Database Access

```python
from sqlalchemy import create_engine, select
from dpm import DPM, TableVersionCell, Cell

# Connect to the SQLite database
engine = create_engine("sqlite:///dpm.sqlite")

# Type-safe queries with IDE support
with engine.connect() as conn:
    # Query with autocompletion and type checking
    stmt = select(TableVersionCell).where(TableVersionCell.CellContent.isnot(None))
    result = conn.execute(stmt)
    
    for row in result:
        print(f"Cell ID: {row.CellID}, Content: {row.CellContent}")
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
class TableVersionCell(DPM):
    """Auto-generated model for the TableVersionCell table."""
    __tablename__ = "TableVersionCell"

    CellID: Mapped[str] = mapped_column(primary_key=True)
    TableVersionCellID: Mapped[str]
    CellContent: Mapped[str | None]  # Nullable column
    IsActive: Mapped[bool]           # Boolean type
    CreatedDate: Mapped[date]        # Date type
    
    # Automatically generated relationships
    Cell: Mapped[Cell] = relationship(foreign_keys=[CellID])
    TableVersionHeader: Mapped[TableVersionHeader] = relationship(
        foreign_keys=[TableVersionHeaderID]
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

OpenDPM is built as a modular workspace with specialized components:

### Project Components

- **`opendpm`**: Central CLI that coordinates all functionality
- **`archive`**: Version management, downloads, and release tracking
- **`convert`**: Access-to-SQLite conversion engine (Windows only)
- **`scrape`**: Automated discovery of new EBA releases
- **`dpm2`**: Generated Python packages (future enhancement)

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
git clone https://github.com/JimLundin/opendpm.git
cd opendpm

# Install UV package manager
pip install uv

# Install all dependencies
uv sync

# Install in development mode
uv pip install -e .
```

### Project Structure

OpenDPM uses a UV workspace with multiple subprojects:

```
opendpm/
├── src/opendpm/           # Main CLI package
├── projects/              # Workspace subprojects
│   ├── archive/          # Version management & downloads
│   ├── convert/          # Access-to-SQLite conversion
│   ├── scrape/           # Web scraping for new versions
│   └── dpm2/             # Generated Python packages (placeholder)
├── .github/workflows/    # CI/CD automation
└── pyproject.toml        # Workspace configuration
```

### Working with Subprojects

Each subproject is independently installable:

```bash
# Install specific subproject
uv pip install -e projects/archive
uv pip install -e projects/convert  # Windows only
uv pip install -e projects/scrape
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
