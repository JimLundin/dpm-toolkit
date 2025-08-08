# Publishing Workflows with UV

This document outlines the publishing workflows for the OpenDPM workspace using UV commands.

## Overview

The OpenDPM workspace contains two packages that require publishing:

1. **opendpm** - Main CLI tool (published to PyPI)
2. **dpm2** - Generated Python package from conversion artifacts (requires regeneration before publishing)

The other workspace projects (`archive`, `migrate`, `scrape`) are internal dependencies and should not be published separately.

## Prerequisites

- UV installed and configured
- PyPI account and API token configured
- Windows environment for conversion operations (dpm2 regeneration)

## Publishing Workflows

### 1. Publishing the Main CLI Tool (opendpm)

#### Manual Publishing

```bash
# 1. Ensure you're in the workspace root
cd /path/to/opendpm

# 2. Update version in pyproject.toml if needed
# Edit the version field in the root pyproject.toml

# 3. Sync dependencies and lock file
uv sync

# 4. Run tests (if available)
uv run pytest  # or your test command

# 5. Build the package
uv build

# 6. Publish to PyPI
uv publish
```

#### Automated GitHub Actions Workflow

The workflow is available at `.github/workflows/publish-opendpm.yml` and will:
- Trigger automatically on GitHub releases
- Can be manually triggered from the Actions tab
- Build and publish the opendpm CLI to PyPI using UV commands

### 2. Publishing the DPM2 Package

**Important**: The dpm2 package must be regenerated using the latest conversion artifacts before each publish.

#### Manual Publishing

```bash
# 1. Ensure you're on Windows (required for conversion)
# 2. Navigate to workspace root
cd /path/to/opendpm

# 3. Regenerate dpm2 package using the CLI
uv run opendpm migrate --output projects/dpm2/src/dpm2/

# 4. Update version in projects/dpm2/pyproject.toml if needed

# 5. Navigate to dpm2 project
cd projects/dpm2

# 6. Build the dpm2 package
uv build

# 7. Publish to PyPI
uv publish
```

#### Automated GitHub Actions Workflow

The workflow is available at `.github/workflows/publish-dpm2.yml` and will:
- Run on Windows (required for Access DB conversion)
- Regenerate the dpm2 package with latest conversion artifacts
- Build and publish to PyPI
- Must be triggered manually from the Actions tab

## Configuration

### PyPI Token Setup

1. Generate an API token from PyPI
2. Add it as a repository secret named `PYPI_API_TOKEN`
3. UV will automatically use the `UV_PUBLISH_TOKEN` environment variable

### Version Management

Update versions manually in the respective `pyproject.toml` files:
- **opendpm**: Edit version in root `pyproject.toml`
- **dpm2**: Edit version in `projects/dpm2/pyproject.toml`

Ensure version numbers are incremented before each publish to avoid conflicts.

## Testing Before Publishing

### Test Installation

```bash
# Test local installation
uv pip install -e .

# Test specific project
uv pip install -e projects/dpm2/

# Test built wheel
uv pip install dist/opendpm-*.whl
```

### Publish to Test PyPI First

```bash
# Publish to Test PyPI
uv publish --repository testpypi

# Test installation from Test PyPI
uv pip install --index-url https://test.pypi.org/simple/ opendpm
```

## Troubleshooting

### Common Issues

1. **Build failures**: Ensure all dependencies are properly specified in `pyproject.toml`
2. **Publishing failures**: Check PyPI token permissions and package name availability
3. **DPM2 regeneration**: Must be run on Windows with Access DB drivers installed
4. **Version conflicts**: Ensure version numbers are incremented before publishing

### UV-Specific Commands

```bash
# Check workspace status
uv tree

# Validate pyproject.toml
uv check

# Clean build artifacts (PowerShell on Windows)
Remove-Item -Recurse -Force dist/, build/, *.egg-info/ -ErrorAction SilentlyContinue

# Clean build artifacts (Bash/Linux)
rm -rf dist/ build/ *.egg-info/

# List available UV commands
uv --help
```

## Security Considerations

- Store PyPI tokens as GitHub secrets, never in code
- Use scoped tokens when possible
- Regularly rotate API tokens
- Review published packages for sensitive information

## Release Checklist

### For opendpm CLI:
- [ ] Update version in root `pyproject.toml`
- [ ] Update CHANGELOG.md
- [ ] Run tests
- [ ] Build and test locally
- [ ] Create GitHub release
- [ ] Verify PyPI publication

### For dpm2 package:
- [ ] Ensure Windows environment
- [ ] Regenerate package with latest conversion
- [ ] Update version in `projects/dpm2/pyproject.toml`
- [ ] Test generated package
- [ ] Build and publish
- [ ] Verify PyPI publication
