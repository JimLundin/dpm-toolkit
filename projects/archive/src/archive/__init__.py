"""Module for loading and managing version information."""

from archive.download import download_source, extract_archive
from archive.versions import (
    Source,
    SourceType,
    Version,
    Group,
    compare_version_urls,
    get_source,
    get_version,
    get_version_urls,
    get_versions,
    get_versions_by_type,
    latest_version,
)

__all__ = [
    "Source",
    "SourceType",
    "Version",
    "Group",
    "compare_version_urls",
    "download_source",
    "extract_archive",
    "get_source",
    "get_version",
    "get_version_urls",
    "get_versions",
    "get_versions_by_type",
    "latest_version",
]
