"""Module for downloading and managing Access database files."""

from __future__ import annotations

import logging
import tomllib
from io import BytesIO
from typing import TYPE_CHECKING
from zipfile import ZipFile, ZipInfo

import requests

if TYPE_CHECKING:
    from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def load_config(config: Path) -> dict[str, str]:
    """Load database source URLs from the config file."""
    with config.open("rb") as f:
        return tomllib.load(f)


def download_archive(url: str) -> BytesIO:
    """Download a zip file containing Access databases."""
    logger.info("Downloading from %s", url)
    response = requests.get(url, timeout=60, allow_redirects=False)
    response.raise_for_status()

    return BytesIO(response.content)


def find_access_database(archive: ZipFile) -> ZipInfo | None:
    """Find Access database in a zip file."""
    for zip_info in archive.infolist():
        if zip_info.filename.endswith(".accdb"):
            return zip_info

    return None


def fetch_databases(config: Path, target: Path) -> None:
    """Download all database files specified in the config.

    Args:
        config: Path to the sources.toml config file
        target: Directory to save downloaded databases

    """
    target.mkdir(parents=True, exist_ok=True)
    sources = load_config(config)
    for version, url in sources.items():
        try:
            archive_data = download_archive(url)
            with ZipFile(archive_data) as archive:
                if db_file := find_access_database(archive):
                    db_file.filename = f"dpm_{version}.accdb"
                    archive.extract(db_file, target)
                    logger.info("Downloaded version %s", version)

        except Exception:
            logger.exception("Failed to download version %s", version)
