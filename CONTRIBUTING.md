# Contributing to DPM Toolkit

Thanks for your interest in contributing! This document provides basic guidelines for contributing to DPM Toolkit.

## Development Setup

```bash
# Clone and setup
git clone https://github.com/JimLundin/dpm-toolkit.git
cd dpm-toolkit

# Install UV package manager
pip install uv

# Install all dependencies
uv sync

# Install in development mode
uv pip install -e .
```

## Code Quality

Before submitting changes, ensure your code passes all quality checks:

```bash
# Run linting and auto-fix
ruff check --fix
ruff format

# Run type checking
mypy src/
pyright src/
```

## Project Structure

DPM Toolkit contains focused internal modules and separate data packages:

- **`src/dpm_toolkit/`** - CLI and internal feature modules
- **`src/dpm_toolkit/archive/`** - Version management and downloads
- **`src/dpm_toolkit/migrate/`** - Database conversion (Windows only)
- **`src/dpm_toolkit/scrape/`** - Web scraping for new versions
- **`src/dpm_toolkit/schema/`** - Python model generation
- **`src/dpm_toolkit/compare/`** - Database comparison reports
- **`src/dpm_toolkit/analysis/`** - Type refinement analysis
- **`tests/<module>/`** - Tests for the matching internal module
- **`projects/dpm2/`** - Generated models package (separate distribution)
- **`projects/dpmlite/`** - Lightweight data package (separate distribution)

The internal modules are bundled into the single `dpm-toolkit` distribution and
gated behind optional extras. `projects/dpm2` and `projects/dpmlite` are
separate workspace packages published on their own.

## Making Changes

1. **Create a branch** for your changes
2. **Make focused commits** - one logical change per commit
3. **Test your changes** - ensure functionality works as expected
4. **Run quality checks** - all code must pass linting and type checking
5. **Update documentation** - update relevant README files if needed

## Platform Considerations

- **Migration features require Windows** due to Microsoft Access ODBC drivers
- **Most functionality works cross-platform** (macOS, Linux, Windows)
- **CI/CD pipelines handle Windows-specific operations**

## Submitting Changes

1. **Push your branch** to your fork
2. **Open a Pull Request** with a clear description
3. **Respond to feedback** and make requested changes
4. **Ensure CI passes** - all automated checks must pass

## Questions?

- **Issues**: Report bugs or request features via GitHub Issues
- **Discussions**: Ask questions in GitHub Discussions
- **Documentation**: Check existing READMEs for guidance

We appreciate your contributions to making EBA DPM data more accessible!
