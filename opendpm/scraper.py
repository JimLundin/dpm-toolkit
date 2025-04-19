"""Module for scraping and managing Access database files."""

from bs4 import BeautifulSoup
from requests import get

EBA_URL = "https://www.eba.europa.eu"
REPORTING_FRAMEWORKS_URL = f"{EBA_URL}/risk-and-data-analysis/reporting-frameworks"


def get_reporting_frameworks() -> set[str]:
    """Get a list of reporting frameworks."""
    response = get(REPORTING_FRAMEWORKS_URL, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")
    return {
        link.get("href")
        for link in soup.find_all("a")
        if "reporting-framework" in link.get("href")
    }


def main() -> None:
    """Test the scraper."""
    print(get_reporting_frameworks())


if __name__ == "__main__":
    main()
