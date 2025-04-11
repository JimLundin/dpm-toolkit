"""Module for loading and managing version information."""

from datetime import date
from hashlib import sha256
from logging import getLogger
from pathlib import Path
from tomllib import load
from typing import Literal, TypedDict

logger = getLogger(__name__)


class Hash(TypedDict):
    """Hash of a file."""

    type: Literal["sha256"]
    value: str


class Base(TypedDict):
    """Version of a database."""

    id: str
    url: str
    hash: Hash
    release_date: date


class Release(Base):
    """Version of a database."""

    type: Literal["release"]


class Beta(Base):
    """Version of a database."""

    type: Literal["beta"]
    superseded_by: str


type Version = Release | Beta

type Releases = list[Release]
type Betas = list[Beta]
type Versions = list[Version]

VERSION_FILE = Path(__file__).parent / "versions.toml"


def get_versions() -> Versions:
    """Load database source URLs from the config file."""
    with VERSION_FILE.open("rb") as f:
        versions: Versions = load(f)["versions"]
        return versions


def get_releases(versions: Versions) -> Releases:
    """Get the releases from the given versions."""
    return [version for version in versions if version["type"] == "release"]


def latest_release(releases: Releases) -> Release:
    """Get the latest release from the given versions."""
    return max(releases, key=lambda version: version["release_date"])


def verify_version(content: bytes, version_hash: Hash) -> bool:
    """Verify the content against the hash provided in the version."""
    if version_hash["type"] == "sha256":
        hasher = sha256(content)
        return version_hash["value"] == hasher.hexdigest()

    return False


def render_version(version: Version) -> str:
    """Render a version string."""
    return f"{version['id']} ({version['release_date']})"
