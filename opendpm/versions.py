"""Module for loading and managing version information."""

from collections.abc import Sequence
from datetime import date
from pathlib import Path
from tomllib import load
from typing import Literal, TypedDict


class Source(TypedDict):
    """Source of a database."""

    url: str
    hash: str


class Version(TypedDict):
    """Version of a database."""

    id: str
    release_date: date
    type: Literal["release", "draft"]
    access: Source
    sqlite: Source


type Versions = Sequence[Version]

VERSION_FILE = Path(__file__).parent / "versions.toml"


def get_versions() -> Versions:
    """Load versions from the config file."""
    with VERSION_FILE.open("rb") as f:
        versions: Versions = load(f)["versions"]
        return versions


def get_releases(versions: Versions) -> Versions:
    """Get the releases from the given versions."""
    return [version for version in versions if version["type"] == "release"]


def get_drafts(versions: Versions) -> Versions:
    """Get the drafts from the given versions."""
    return [version for version in versions if version["type"] == "draft"]


def latest_version(versions: Versions) -> Version:
    """Get the latest version from the given versions."""
    return max(versions, key=lambda version: version["release_date"])


def get_version(versions: Versions, version_id: str) -> Version | None:
    """Get the version with the given ID."""
    return next((v for v in versions if v["id"] == version_id), None)
