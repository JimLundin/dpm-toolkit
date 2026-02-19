"""Scrape EBA reporting framework pages for DPM 2.0 database download URLs."""

from scrape.scraper import (
    get_active_reporting_frameworks,
    get_dpm_urls,
    get_framework_urls,
)

__all__ = [
    "get_active_reporting_frameworks",
    "get_dpm_urls",
    "get_framework_urls",
]
