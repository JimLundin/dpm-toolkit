# Scrape

Web scraping tools for automated discovery of new EBA DPM releases.

## Purpose

The scrape module provides:
- Automated discovery of new EBA releases
- Parsing of EBA website content
- Detection of updated reporting frameworks
- Integration with CI/CD pipelines for automated updates

## Key Functions

- `get_active_reporting_frameworks()` - Discover active EBA reporting frameworks and versions

## Usage

```python
from scrape import get_active_reporting_frameworks

# Discover new EBA releases
frameworks = get_active_reporting_frameworks()
for framework in frameworks:
    print(f"Found: {framework['name']} - {framework['version']}")
```

## Features

- **Automated Discovery** - Finds new EBA releases without manual intervention
- **Web Parsing** - Handles EBA website structure and content formats
- **CI/CD Integration** - Designed for automated pipeline execution
- **Change Detection** - Identifies when new versions are available

## Dependencies

- `beautifulsoup4` - HTML parsing
- `requests` - HTTP requests

This is an internal OpenDPM component - use via `opendpm scrape` command or automated CI/CD workflows.