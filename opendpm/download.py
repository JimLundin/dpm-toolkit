"""Module for downloading and managing Access database files."""

from __future__ import annotations

from io import BytesIO
from logging import getLogger
from typing import TYPE_CHECKING
from zipfile import ZipFile, ZipInfo

from requests import get

from opendpm.versions import verify_version

if TYPE_CHECKING:
    from pathlib import Path

    from opendpm.versions import Version

logger = getLogger(__name__)


def download_url(url: str) -> bytes:
    """Download the zip file containing the DPM database."""
    logger.info("Downloading from %s", url)
    response = get(url, timeout=30, allow_redirects=False)
    response.raise_for_status()

    return response.content


def find_access_database(archive: ZipFile) -> ZipInfo | None:
    """Find Access database in a zip file."""
    for zip_info in archive.infolist():
        if zip_info.filename.endswith(".accdb"):
            return zip_info

    return None


def extract_database(archive: BytesIO, name: str, target: Path) -> None:
    """Extract Access database from the archive to the target with the given name."""
    with ZipFile(archive) as zip_file:
        if db_file := find_access_database(zip_file):
            db_file.filename = f"dpm_{name}.accdb"
            target.mkdir(parents=True, exist_ok=True)
            zip_file.extract(db_file, target)


def fetch_version(version: Version, target: Path) -> None:
    """Download and extract database file specified in the version.

    Args:
        version: Version to download
        target: Directory to save downloaded databases

    """
    version_bytes = download_url(version["url"])
    if not verify_version(version_bytes, version["hash"]):
        logger.warning("Hash verification failed")
    archive_data = BytesIO(version_bytes)
    extract_database(archive_data, version["id"], target)
    logger.info("Downloaded version %s", version["id"])
