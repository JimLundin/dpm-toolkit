"""Module for loading and managing version information."""

from collections import defaultdict
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from tomllib import load
from typing import Literal, NotRequired, TypedDict

type VersionUrls = dict[str, set[str]]


type VersionType = Literal["sample", "draft", "final", "release", "errata", "hotfix"]

type Group = Literal["all", "release", "draft"]

type SourceType = Literal["original", "archive", "converted"]


class Source(TypedDict):
    """Source of a database."""

    url: str
    checksum: str


class Version(TypedDict):
    """Version of a database."""

    id: str
    previous: NotRequired[str]
    version: str
    type: VersionType
    semver: str
    date: date
    revision: NotRequired[int]
    original: Source
    archive: Source
    converted: Source


def get_source(version: Version, source_type: SourceType) -> Source:
    """Get source by type from version."""
    try:
        return version[source_type]
    except KeyError as err:
        msg = f"Unknown source type: {source_type}"
        raise ValueError(msg) from err


type Versions = Iterable[Version]

VERSION_FILE = Path(__file__).parent / "versions.toml"


def get_versions() -> Versions:
    """Load versions from the config file."""
    with VERSION_FILE.open("rb") as f:
        versions: Versions = load(f)["versions"]
        return versions


def get_versions_by_type(versions: Versions, group: Group) -> Versions:
    """Get the versions of the given type."""
    if group == "all":
        return versions
    if group == "release":
        return (v for v in versions if v["type"] in ("release", "errata", "hotfix"))
    if group == "draft":
        return (v for v in versions if v["type"] in ("draft", "sample"))
    msg = f"Unknown version group: {group}"
    raise ValueError(msg)


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
        if original_source := version.get("original"):
            version_urls[version["version"]].add(original_source["url"])

    return version_urls


def compare_version_urls(new_urls: VersionUrls) -> VersionUrls:
    """Compare new URLs with existing version URLs."""
    version_urls = get_version_urls()

    return {
        version: new_urls - version_urls.get(version, set())
        for version, new_urls in new_urls.items()
        if new_urls - version_urls.get(version, set())
    }
