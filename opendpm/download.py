"""Module for downloading and managing Access database files."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING
from zipfile import ZipFile

from requests import get

if TYPE_CHECKING:
    from pathlib import Path

    from opendpm.versions import Source


def download_url(url: str) -> bytes:
    """Download the zip file containing the DPM database."""
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
    if sha256(version_bytes).hexdigest() != source["hash"]:
        print("Hash verification failed")
    else:
        print("Hash verification successful")
    return BytesIO(version_bytes)
