"""Module for loading and managing version information."""

from collections.abc import Sequence
from datetime import date
from hashlib import sha256
from logging import getLogger
from pathlib import Path
from tomllib import load
from typing import Literal, NotRequired, TypedDict

logger = getLogger(__name__)


class Hash(TypedDict):
    """Hash of a file."""

    type: Literal["sha256"]
    value: str


class Source(TypedDict):
    """Source of a database."""

    url: str
    hash: Hash


class Base(TypedDict):
    """Version of a database."""

    id: str
    release_date: date
    access: Source
    sqlite: NotRequired[Source]


class Release(Base):
    """Version of a database."""

    type: Literal["release"]


class Draft(Base):
    """Version of a database."""

    type: Literal["draft"]
    superseded_by: str


type Version = Release | Draft

type Releases = Sequence[Release]
type Drafts = Sequence[Draft]
type Versions = Sequence[Version]

VERSION_FILE = Path(__file__).parent / "versions.toml"


def get_versions() -> Versions:
    """Load database source URLs from the config file."""
    with VERSION_FILE.open("rb") as f:
        versions: Versions = load(f)["versions"]
        return versions


def get_releases(versions: Versions) -> Releases:
    """Get the releases from the given versions."""
    return [version for version in versions if version["type"] == "release"]


def get_drafts(versions: Versions) -> Drafts:
    """Get the drafts from the given versions."""
    return [version for version in versions if version["type"] == "draft"]


def latest_version(versions: Versions) -> Version:
    """Get the latest version from the given versions."""
    return max(versions, key=lambda version: version["release_date"])


def get_version(versions: Versions, version_id: str) -> Version | None:
    """Get the version with the given ID."""
    return next((v for v in versions if v["id"] == version_id), None)


def verify_source(content: bytes, source_hash: Hash) -> bool:
    """Verify the content against the hash provided in the source."""
    if source_hash["type"] == "sha256":
        hasher = sha256(content)
        return source_hash["value"] == hasher.hexdigest()

    return False
