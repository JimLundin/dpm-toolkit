"""Scrape EBA reporting framework pages for DPM 2.0 database download URLs."""

import logging
import re
from typing import Final
from urllib.parse import unquote, urljoin

from bs4 import BeautifulSoup, Tag
from requests import Session
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL: Final[str] = "https://www.eba.europa.eu"
FRAMEWORKS_URL: Final[str] = f"{BASE_URL}/risk-and-data-analysis/reporting-frameworks"
FRAMEWORK_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"reporting-framework-(\d+)$",
)

# Two patterns to detect DPM 2.0 database references (case-insensitive):
#   1) URL filenames: DPM2.0_release.zip, dpm_2.0_release.zip, DPM 2.0.zip
#   2) Link text:     "DPM database 2.0"
DPM2_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"dpm[_ ]?2", re.IGNORECASE),
    re.compile(r"dpm\s+database\s+2", re.IGNORECASE),
)

EXCLUDE_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"glossary", re.IGNORECASE),
    re.compile(r"conversion", re.IGNORECASE),
    re.compile(r"table.layout", re.IGNORECASE),
    re.compile(r"categorization", re.IGNORECASE),
)

USER_AGENT: Final[str] = (
    "Mozilla/5.0 (compatible; dpm-toolkit/1.0; "
    "+https://github.com/JimLundin/dpm-toolkit)"
)

MIN_VERSION: Final[float] = 3.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_session() -> Session:
    """Return a :class:`requests.Session` with a proper ``User-Agent``."""
    session = Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def _fetch_page(session: Session, url: str) -> BeautifulSoup:
    """Fetch *url* and return the parsed HTML tree."""
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def _parse_version(digits: str) -> str:
    """Convert a digit string into a dotted version (``"42"`` → ``"4.2"``)."""
    if len(digits) < 2:  # noqa: PLR2004
        return digits
    return f"{digits[0]}.{digits[1:]}"


def _is_dpm2_database(href: str, text: str) -> bool:
    """Return *True* when *href*/*text* point to a DPM 2.0 database ZIP."""
    decoded = unquote(href)
    if not decoded.lower().endswith(".zip"):
        return False
    combined = f"{decoded} {text}"
    if not any(p.search(combined) for p in DPM2_PATTERNS):
        return False
    return not any(p.search(combined) for p in EXCLUDE_PATTERNS)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_framework_urls(session: Session | None = None) -> dict[str, str]:
    """Discover reporting framework page URLs from the EBA website.

    Returns a mapping of version strings (e.g. ``"4.2"``) to the absolute
    URL of the corresponding framework page.
    """
    if session is None:
        session = _create_session()

    soup = _fetch_page(session, FRAMEWORKS_URL)
    frameworks: dict[str, str] = {}

    for tag in soup.find_all("a", href=True):
        if not isinstance(tag, Tag):
            continue
        href = tag.get("href")
        if not isinstance(href, str):
            continue

        match = FRAMEWORK_PATTERN.search(href)
        if match:
            version = _parse_version(match.group(1))
            frameworks[version] = urljoin(BASE_URL, href)

    return frameworks


def get_dpm_urls(session: Session, framework_url: str) -> set[str]:
    """Extract DPM 2.0 database download URLs from a framework page."""
    soup = _fetch_page(session, framework_url)
    urls: set[str] = set()

    for tag in soup.find_all("a", href=True):
        if not isinstance(tag, Tag):
            continue
        href = tag.get("href")
        if not isinstance(href, str):
            continue

        text = tag.get_text(strip=True)
        full_url = urljoin(framework_url, href)

        if _is_dpm2_database(full_url, text):
            urls.add(full_url)

    return urls


def get_active_reporting_frameworks() -> dict[str, set[str]]:
    """Discover DPM 2.0 database URLs across active EBA reporting frameworks.

    Scans each framework page whose version is ``>= MIN_VERSION`` and returns
    a mapping of version strings to the set of DPM database download URLs
    found on that page.
    """
    session = _create_session()
    frameworks = get_framework_urls(session)

    results: dict[str, set[str]] = {}
    for version, url in sorted(frameworks.items()):
        try:
            if float(version) < MIN_VERSION:
                continue
        except ValueError:
            continue

        logger.info("Scanning framework %s: %s", version, url)
        try:
            dpm_urls = get_dpm_urls(session, url)
        except RequestException:
            logger.warning("Failed to fetch framework %s page", version)
            continue

        if dpm_urls:
            results[version] = dpm_urls

    return results
