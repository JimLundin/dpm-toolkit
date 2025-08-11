"""Module for downloading and managing Access database files."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from logging import getLogger
from typing import TYPE_CHECKING
from zipfile import ZipFile

from requests import get

if TYPE_CHECKING:
    from pathlib import Path

    from archive import Source

logger = getLogger(__name__)


def verify_checksum(data: bytes, checksum: str) -> bool:
    """Verify the checksum of the data."""
    if not checksum.startswith("sha256:"):
        logger.error("Invalid checksum format: %s", checksum)
        return False

    return checksum == f"sha256:{sha256(data).hexdigest()}"


def download_source(source: Source) -> BytesIO:
    """Download the zip file containing the DPM database."""
    response = get(source["url"], timeout=30, allow_redirects=False)
    response.raise_for_status()

    if checksum := source.get("checksum"):
        if not verify_checksum(response.content, checksum):
            logger.warning("Checksum verification failed")
    else:
        logger.warning("No checksum provided")

    return BytesIO(response.content)


def extract_archive(archive: BytesIO, target: Path) -> None:
    """Extract files from the archive to the target with the given name."""
    with ZipFile(archive) as zip_file:
        target.mkdir(parents=True, exist_ok=True)
        zip_file.extractall(target)
