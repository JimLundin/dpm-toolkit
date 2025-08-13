# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Package Management
- `uv sync` - Install all dependencies and sync the workspace
- `uv pip install -e .` - Install main package in development mode
- `uv pip install -e projects/archive` - Install specific subproject
- `uv pip install -e projects/migrate` - Install migration tools (Windows only)
- `uv pip install -e projects/scrape` - Install scraping tools

### Code Quality
- `ruff check --fix` - Run linting and auto-fix issues
- `ruff format` - Format code according to project standards
- `mypy src/` - Run type checking with mypy
- `pyright src/` - Run type checking with pyright

### Testing
- `uv run pytest` - Run tests (when available)

### Application Commands
- `opendpm list` - List available database versions
- `opendpm download --version release --type converted` - Download latest stable release
- `opendpm update` - Find new EBA releases
- `opendpm schema --source SOURCE --target TARGET` - Generate Python models from SQLite
- `opendpm migrate --source SOURCE --target TARGET` - Migrate Access to SQLite (Windows only)

## Architecture Overview

OpenDPM is a UV workspace with multiple specialized subprojects:

### Main Package (`src/opendpm/`)
- **CLI Interface**: `cli.py` contains the main command-line interface using argparse
- **Entry Point**: `__main__.py` provides the package entry point
- **Core Functionality**: Coordinates between subprojects via imports

### Workspace Subprojects (`projects/`)
- **`archive/`**: Version management, downloads, and release tracking
- **`migrate/`**: Access-to-SQLite migration engine (Windows-only, requires ODBC drivers)
- **`scrape/`**: Automated discovery of new EBA releases via web scraping
- **`schema/`**: Python model generation from SQLite databases
- **`dpm2/`**: Generated Python packages (future enhancement placeholder)

### Key Design Patterns
- **Workspace Architecture**: Uses UV workspace with `[tool.uv.workspace]` for managing multiple related packages
- **Optional Dependencies**: Platform-specific functionality (like migration) is optional via `[project.optional-dependencies]`
- **Dynamic Imports**: CLI uses try/except imports to gracefully handle missing optional dependencies
- **Type Safety**: Strict typing with mypy and pyright, comprehensive type annotations

### Data Flow
1. **Version Discovery**: `scrape` finds new EBA releases
2. **Download Management**: `archive` handles version tracking and downloads
3. **Migration Pipeline**: `migrate` transforms Access databases to SQLite with type-safe Python models
4. **CLI Coordination**: Main package orchestrates functionality across subprojects

### Important Constraints
- **Platform Dependency**: Migration requires Windows due to Microsoft Access ODBC drivers
- **UV Build System**: Uses `uv_build` as build backend instead of standard setuptools
- **Strict Code Quality**: All code must pass ruff linting, mypy, and pyright type checking
- **Python Version**: Requires Python 3.13+

### Testing and CI/CD
- GitHub Actions pipeline automatically detects new EBA releases
- Windows runners handle database migration due to platform requirements
- Generated artifacts are published as GitHub releases
- CLI provides both direct download and release-based distribution