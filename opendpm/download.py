"""Module for downloading and managing Access database files."""

from __future__ import annotations

from enum import StrEnum, auto
from io import BytesIO
from logging import getLogger
from typing import TYPE_CHECKING
from zipfile import ZipFile

from requests import get

from opendpm.versions import verify_source

if TYPE_CHECKING:
    from pathlib import Path

    from opendpm.versions import Source, Version

logger = getLogger(__name__)


class Types(StrEnum):
    """Types of database files."""

    ACCESS = auto()
    SQLITE = auto()


def download_url(url: str) -> bytes:
    """Download the zip file containing the DPM database."""
    logger.info("Downloading from %s", url)
    response = get(url, timeout=30, allow_redirects=False)
    response.raise_for_status()

    return response.content


def extract_database(archive: BytesIO, target: Path) -> None:
    """Extract Access database from the archive to the target with the given name."""
    with ZipFile(archive) as zip_file:
        target.mkdir(parents=True, exist_ok=True)
        zip_file.extractall(target)


def fetch_source(source: Source) -> BytesIO:
    """Download and extract database file specified by the source."""
    version_bytes = download_url(source["url"])
    if not verify_source(version_bytes, source["hash"]):
        logger.warning("Hash verification failed")
    return BytesIO(version_bytes)


def fetch_version(version: Version, target: Path, source_type: Types) -> None:
    """Download and extract database file specified by the version.

    Args:
        version: Version to download from
        target: Directory to save downloaded databases.
        source_type: Type of database to download

    """
    if source_type == Types.ACCESS:
        archive_data = fetch_source(version["access"])
    elif source_type == Types.SQLITE:
        if "sqlite" not in version:
            logger.error("SQLite source not found for version %s", version["id"])
            return
        archive_data = fetch_source(version["sqlite"])
    else:
        msg = f"Invalid source type: {source_type}"
        raise ValueError(msg)
    target_folder = target / version["id"]
    extract_database(archive_data, target_folder)
    logger.info("Downloaded version %s", version["id"])
