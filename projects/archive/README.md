# Archive

Version management, downloads, and release tracking for DPM Toolkit.

## Purpose

The archive module handles:
- Loading version information from configuration files
- Downloading source files from remote URLs
- Extracting and managing archived content
- Comparing and filtering versions

## Key Functions

- `get_versions()` - Load all available versions
- `latest_version()` - Get the most recent version
- `download_source()` - Download files from URLs
- `extract_archive()` - Extract compressed archives

## Usage

```python
from archive import get_versions, latest_version, download_source

# Get all versions
versions = get_versions()

# Get latest version
latest = latest_version()

# Download a source file
download_source("https://example.com/file.zip", "/path/to/destination")
```

## Dependencies

- `requests` - HTTP downloads

This is an internal DPM Toolkit component - install the main `dpm-toolkit` package instead of using this directly.