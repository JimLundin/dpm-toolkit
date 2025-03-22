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


def load_sources(config_file: Path) -> dict[str, str]:
    """Load database source URLs from the config file."""
    with config_file.open("rb") as f:
        return tomllib.load(f)


def download(url: str) -> BytesIO:
    """Download a zip file containing Access databases."""
    logger.info("Downloading from %s", url)
    response = requests.get(url, timeout=60, allow_redirects=False)
    response.raise_for_status()

    return BytesIO(response.content)


def extract_databases(zip_file: ZipFile) -> ZipInfo | None:
    """Extract Access databases from a zip file."""
    for zip_info in zip_file.infolist():
        if zip_info.filename.endswith(".accdb"):
            return zip_info

    return None


def download_databases(config_file: Path, target_dir: Path) -> None:
    """Download all database files specified in the config.

    Args:
        config_file: Path to the sources.toml config file
        target_dir: Directory to save downloaded databases

    """
    target_dir.mkdir(parents=True, exist_ok=True)
    sources = load_sources(config_file)
    for version, url in sources.items():
        try:
            zip_bytes = download(url)
            with ZipFile(zip_bytes) as zip_file:
                if zip_db := extract_databases(zip_file):
                    zip_db.filename = f"dpm_{version}.accdb"
                    zip_file.extract(zip_db, target_dir)
                    logger.info("Downloaded version %s", version)

        except Exception:
            logger.exception("Failed to download version %s", version)
