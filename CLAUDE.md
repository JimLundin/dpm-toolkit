# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Package Management
- `uv sync` - Install all dependencies and sync the workspace
- `uv pip install -e .` - Install main package in development mode
- `uv pip install -e projects/archive` - Install specific subproject
- `uv pip install -e projects/migrate` - Install migration tools (Windows only)
- `uv pip install -e projects/scrape` - Install scraping tools
- `uv pip install -e projects/compare` - Install database comparison tools

### Code Quality
- `ruff check --fix` - Run linting and auto-fix issues
- `ruff format` - Format code according to project standards
- `mypy src/` - Run type checking with mypy
- `pyright src/` - Run type checking with pyright

### Testing
- `uv run pytest` - Run tests (when available)
- `uv run pytest projects/compare/tests/` - Run tests for specific subproject

### Application Commands
- `dpm-toolkit versions` - List available database versions (new Typer CLI)
- `dpm-toolkit download VERSION` - Download database version with options
- `dpm-toolkit migrate SOURCE` - Migrate Access to SQLite (Windows only, target defaults to CWD)
- `dpm-toolkit schema SOURCE` - Generate Python models from SQLite (output defaults to CWD)
- `dpm-toolkit compare SOURCE TARGET` - Compare two SQLite databases (uses positional arguments)

## Architecture Overview

DPM Toolkit is a UV workspace with multiple specialized subprojects built around EBA DPM database processing:

### Main Package (`src/dpm_toolkit/`)
- **CLI Interface**: `cli.py` uses modern Typer framework with type-annotated commands
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
- **Output Formats**: JSON and HTML reports with Jinja2 templating

### CLI Design Patterns
- **Modern Typer**: Uses `Annotated` types and `typer.Option()` for clean command definitions
- **Type Safety**: Full type annotations with `TYPE_CHECKING` imports
- **Error Handling**: Consistent `typer.Exit(1)` with proper error messages
- **Optional Dependencies**: Graceful handling of missing subproject imports
- **Consistent UX**: Standard `--quiet`, `--verbose`, and format options across commands

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
4. **Comparison Engine**: `compare` provides schema and data change detection between database versions
5. **CLI Coordination**: Main package orchestrates functionality across subprojects with modern Typer interface

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