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

DPM Toolkit is a UV workspace with these components:

- **`src/dpm_toolkit/`** - Main CLI package
- **`projects/archive/`** - Version management and downloads  
- **`projects/migrate/`** - Database conversion (Windows only)
- **`projects/scrape/`** - Web scraping for new versions
- **`projects/schema/`** - Python model generation
- **`projects/dpm2/`** - Generated models package

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
- **Documentation**: Check existing READMEs and CLAUDE.md for guidance

We appreciate your contributions to making EBA DPM data more accessible!