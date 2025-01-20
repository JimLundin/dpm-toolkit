"""Module for downloading and managing Access database files."""

from __future__ import annotations

import io
import logging
import tomllib
import zipfile
from typing import TYPE_CHECKING

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


def download_and_extract(url: str, target_dir: Path) -> None:
    """Download and extract a zip file containing Access databases."""
    target_dir.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Downloading from %s", url)
        response = requests.get(url, timeout=60, allow_redirects=False)
        response.raise_for_status()

        # Extract the file directly from memory
        logger.info("Extracting to %s", target_dir)
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            for zip_info in zip_ref.filelist:
                if zip_info.filename.lower().endswith(".accdb"):
                    zip_ref.extract(zip_info, target_dir)
                    logger.info("Extracted %s", zip_info.filename)

    except (requests.RequestException, zipfile.BadZipFile):
        logger.exception("Failed to process %s", url)
        raise


def download_databases(config_file: Path, target_dir: Path) -> None:
    """Download all database files specified in the config.

    Args:
        config_file: Path to the sources.toml config file
        target_dir: Directory to save downloaded databases

    """
    sources = load_sources(config_file)
    for version, url in sources.items():
        try:
            download_and_extract(url, target_dir)
            logger.info("Successfully downloaded version %s", version)
        except Exception:
            logger.exception("Failed to download version %s", version)
