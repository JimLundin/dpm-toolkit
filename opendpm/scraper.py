"""Module for scraping and managing Access database files."""

import logging
from http import HTTPStatus
from typing import Final, cast

from bs4 import BeautifulSoup, Tag
from requests import Response, get, head

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


def get_final_url(url: str) -> str:
    """Get the final URL after following redirects."""
    response = head(url, allow_redirects=True, timeout=30)
    response.raise_for_status()

    # Follow any 301 (permanent) redirects
    for resp in response.history:
        if resp.status_code == HTTPStatus.MOVED_PERMANENTLY:
            url = resp.headers["location"]

    return url


def normalize_url(url: str) -> str:
    """Normalize a URL by following redirects and standardizing the domain."""
    if not url.startswith(EBA_URL):
        url = f"{EBA_URL}{url}"

    if MID_PATH not in url:
        url = get_final_url(url)

    return url.replace(WWW_EBA_URL, EBA_URL)


def extract_framework_urls(html_content: str) -> list[str]:
    """Extract framework URLs from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")
    urls = [
        link.get("href")
        for link in soup.find_all("a")
        if isinstance(link, Tag) and isinstance(link.get("href"), str)
    ]

    # Filter to include only framework URLs
    return [
        cast("str", url) for url in urls if url is not None and FRAMEWORK_PATTERN in url
    ]


def fetch_frameworks_page() -> Response:
    """Fetch the reporting frameworks page."""
    response = get(REPORTING_FRAMEWORKS_URL, timeout=10)
    response.raise_for_status()
    return response


def get_reporting_frameworks() -> list[str]:
    """Get a list of reporting frameworks."""
    response = fetch_frameworks_page()
    framework_urls = extract_framework_urls(response.text)
    unique_urls = {normalize_url(url) for url in framework_urls}

    return sorted(unique_urls)


def main() -> None:
    """Test the scraper with verbose output."""
    print("\n".join(get_reporting_frameworks()))


if __name__ == "__main__":
    main()
