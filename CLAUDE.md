# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Package Management
- `uv sync` - Install all dependencies and sync the workspace
- `uv sync --extra migrate` - Install with migration tools (Windows only)
- `uv sync --extra schema` - Install with schema generation tools
- `uv sync --extra compare` - Install with database comparison tools
- `uv sync --extra scrape` - Install with web scraping tools

### Code Quality
- `ruff check --fix` - Run linting and auto-fix issues
- `ruff format` - Format code according to project standards
- `mypy src/` - Run type checking with mypy
- `pyright src/` - Run type checking with pyright

### Testing
- `uv run pytest` - Run tests (when available)
- `uv run pytest projects/compare/tests/` - Run tests for specific subproject

### Application Commands

#### Version Management
- `dpm-toolkit versions` - List release versions (default)
- `dpm-toolkit versions --group all --json` - JSON output of all versions
- `dpm-toolkit versions --latest` - Show only the latest release version

#### Database Operations  
- `dpm-toolkit download 4.1-final` - Download latest final release
- `dpm-toolkit download 4.0-release --variant converted --output ./dbs/` - Download converted SQLite version
- `dpm-toolkit migrate database.accdb --target output.sqlite` - Convert Access to SQLite (Windows only)
- `dpm-toolkit schema database.sqlite --output models.py` - Generate SQLAlchemy models

#### Database Comparison (POSIX-style)
- `dpm-toolkit compare old.db new.db > changes.json` - JSON diff to file
- `dpm-toolkit compare old.db new.db --output-format html > report.html` - HTML report
- `dpm-toolkit compare old.db new.db | jq '.[] | select(.name=="users")` - Filter specific table changes

## Architecture Overview

DPM Toolkit is a UV workspace with multiple specialized subprojects built around EBA DPM database processing:

### Main Package (`src/dpm_toolkit/`)
- **CLI Interface**: `cli.py` uses modern Cyclopts framework with type-annotated commands
- **Entry Point**: `__main__.py` provides the package entry point
- **Core Functionality**: Coordinates between subprojects via dynamic imports

### Workspace Subprojects (`projects/`)
- **`archive/`**: Version management, downloads, and release tracking with structured types
- **`migrate/`**: Access-to-SQLite migration engine (Windows-only, requires ODBC drivers)
- **`scrape/`**: Automated discovery of new EBA releases via web scraping
- **`schema/`**: Python model generation from SQLite databases with SQLAlchemy
- **`compare/`**: Database comparison functionality for schema and data change detection
- **`dpm2/`**: Generated Python packages with type-safe SQLAlchemy models

### Compare Module Architecture
The `compare` module uses a streaming, memory-efficient approach:

- **`Inspector`**: Database introspection with cached table/column metadata
- **`types.py`**: Type definitions using `NamedTuple` and `TypedDict` for performance
- **`main.py`**: Core comparison logic with streaming row comparison algorithm
- **Sorting Strategy**: Consistent key hierarchy (RowGUID > PKs > all columns) for reliable comparison
- **Output Formats**: JSON and HTML reports with Jinja2 templating, streamed to stdout for POSIX compatibility

### CLI Design Patterns
- **Modern Cyclopts**: Clean command definitions with focused functionality using type hints
- **Type Safety**: Full type annotations with `TYPE_CHECKING` imports
- **Error Handling**: Consistent `sys.exit(1)` with proper error messages
- **Optional Dependencies**: Graceful handling of missing subproject imports
- **POSIX Philosophy**: Stdout for data, stderr for messages, composable with pipes and redirection
- **Core Functionality Focus**: Essential options only, avoiding feature bloat

### Key Design Patterns
- **Workspace Architecture**: Uses UV workspace with `[tool.uv.workspace]` for managing multiple related packages
- **Optional Dependencies**: Platform-specific functionality (like migration) is optional via `[project.optional-dependencies]`
- **Dynamic Imports**: CLI uses try/except imports to gracefully handle missing optional dependencies
- **Type Safety**: Strict typing with mypy and pyright, comprehensive type annotations
- **Memory Efficiency**: Streaming algorithms for large database comparisons

### Data Flow
1. **Version Discovery**: `scrape` finds new EBA releases
2. **Download Management**: `archive` handles version tracking and downloads with structured metadata
3. **Migration Pipeline**: `migrate` transforms Access databases to SQLite with type-safe Python models
4. **Comparison Engine**: `compare` provides schema and data change detection, streaming output to stdout for composability
5. **CLI Coordination**: Main package orchestrates functionality across subprojects following POSIX conventions

### Important Constraints
- **Platform Dependency**: Migration requires Windows due to Microsoft Access ODBC drivers
- **UV Build System**: Uses `uv_build` as build backend instead of standard setuptools
- **Strict Code Quality**: All code must pass ruff linting, mypy, and pyright type checking
- **Python Version**: Requires Python 3.13+
- **Memory Considerations**: Compare module designed for efficient handling of large databases

### Testing and CI/CD
- GitHub Actions pipeline automatically detects new EBA releases
- Windows runners handle database migration due to platform requirements
- Generated artifacts are published as GitHub releases
- CLI provides both direct download and release-based distribution
- Individual subprojects have their own test suites

## Documentation Maintenance

### Schema Module Documentation
When refactoring the schema module, always update the corresponding README.md file to reflect:
- Current API functions (`sqlite_read_only()`, `sqlite_to_sqlalchemy_schema()`)
- Correct usage examples with actual function signatures
- Generated code structure (snake_case attributes, mapped_column usage)
- Import paths and module organization