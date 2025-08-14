"""Provides a command-line interface for DPM Toolkit."""

import logging

from dpm_toolkit.cli import main

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

__all__ = ["main"]
