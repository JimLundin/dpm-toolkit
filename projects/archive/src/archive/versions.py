"""Module for loading and managing version information."""

from collections import defaultdict
from datetime import date
from pathlib import Path
from tomllib import load
from typing import Literal, NotRequired, TypedDict

type VersionUrls = dict[str, set[str]]


class Source(TypedDict):
    """Source of a database."""

    url: str
    checksum: NotRequired[str]


class Version(TypedDict):
    """Version of a database."""

    id: str
    date: date
    version: str
    type: Literal["sample", "draft", "final", "release", "errata"]
    revision: NotRequired[int]
    replaced_by: NotRequired[str]
    original: Source
    archive: NotRequired[Source]
    converted: NotRequired[Source]


type Versions = list[Version]

VERSION_FILE = Path(__file__).parent / "versions.toml"


def get_versions() -> Versions:
    """Load versions from the config file."""
    with VERSION_FILE.open("rb") as f:
        versions: Versions = load(f)["versions"]
        return versions


def get_versions_by_type(versions: Versions, *version_types: str) -> Versions:
    """Get the versions of the given type."""
    return [v for v in versions if v["type"] in version_types]


def latest_version(versions: Versions) -> Version:
    """Get the latest version from the given versions."""
    return max(versions, key=lambda version: version["date"])


def get_version(versions: Versions, version_id: str) -> Version | None:
    """Get the version with the given ID."""
    return next((v for v in versions if v["id"] == version_id), None)


def get_version_urls() -> VersionUrls:
    """Get version urls from version source."""
    versions = get_versions()
    version_urls: VersionUrls = defaultdict(set)
    for version in versions:
        version_urls[version["version"]].add(version["original"]["url"])

    return version_urls


def compare_version_urls(new_urls: VersionUrls) -> VersionUrls:
    """Compare new URLs with existing version URLs."""
    version_urls = get_version_urls()

    return {
        version: new_urls - version_urls.get(version, set())
        for version, new_urls in new_urls.items()
        if new_urls - version_urls.get(version, set())
    }
