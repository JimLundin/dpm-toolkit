"""Module for scraping and managing Access database files."""

import logging
from collections.abc import Iterable
from http import HTTPStatus
from typing import Final

from bs4 import BeautifulSoup, Tag
from requests import get, head

from opendpm.versions import VersionUrls, compare_version_urls

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
EBA_URL: Final[str] = "https://eba.europa.eu"
WWW_EBA_URL: Final[str] = "https://www.eba.europa.eu"
MID_PATH: Final[str] = "/risk-and-data-analysis/reporting-frameworks"
REPORTING_FRAMEWORKS_URL: Final[str] = f"{EBA_URL}{MID_PATH}"
FRAMEWORK_PATTERN: Final[str] = "reporting-framework-"


def normalize_url(url: str) -> str:
    """Normalize a URL by standardizing the domain."""
    if not url.startswith((EBA_URL, WWW_EBA_URL)):
        url = f"{WWW_EBA_URL}{url}"

    return url.replace(EBA_URL, WWW_EBA_URL)


def get_final_url(url: str) -> str:
    """Get the final URL after following redirects."""
    if MID_PATH in url:
        return url

    response = head(url, allow_redirects=True, timeout=30)

    # Follow any 301 (permanent) redirects
    for resp in response.history:
        if resp.status_code == HTTPStatus.MOVED_PERMANENTLY:
            url = resp.headers["location"]

    return url


def extract_urls(
    url: str,
    match_patterns: Iterable[str] = (),
    exclude_patterns: Iterable[str] = (),
) -> set[str]:
    """Extract framework URLs from HTML content."""
    response = get(url, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")
    urls = [link.get("href") for link in soup.find_all("a") if isinstance(link, Tag)]
    return {
        url
        for url in urls
        if isinstance(url, str)
        and all(match.lower() in url.lower() for match in match_patterns)
        and not any(exclude.lower() in url.lower() for exclude in exclude_patterns)
    }


def get_reporting_frameworks() -> Iterable[str]:
    """Get a list of reporting frameworks."""
    framework_urls = extract_urls(REPORTING_FRAMEWORKS_URL, (FRAMEWORK_PATTERN,))
    final_urls = {normalize_url(url) for url in framework_urls}
    unique_urls = {get_final_url(url) for url in final_urls}
    return sorted(unique_urls)


def get_framework_version(url: str) -> str:
    """Get the version of a reporting framework."""
    match = url.split("-")[-1]
    digits = "".join(c for c in match if c.isdigit())
    return f"{digits[0]}.{digits[1:]}" if digits else ""


def get_new_reporting_frameworks() -> VersionUrls:
    """Get new reporting frameworks."""
    version_url = {
        get_framework_version(url): url for url in get_reporting_frameworks()
    }
    filter_version_url = {
        version: url for version, url in version_url.items() if version[0] > "3"
    }
    found_version_urls = {
        version: extract_urls(url, ("dpm", "2.0", "zip"), ("glossary",))
        for version, url in filter_version_url.items()
    }
    return compare_version_urls(found_version_urls)
