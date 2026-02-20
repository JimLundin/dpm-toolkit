"""Check for new DPM database download URLs not yet tracked in versions.toml.

Compares URLs discovered by the scraper against all known original URLs
in the archive.  Prints JSON when new URLs are found, or a plain message
when everything is up to date.

Exit codes:
    0 — new URLs were found (printed as JSON to stdout)
    1 — no new URLs found
"""

from json import dumps
from sys import exit as sys_exit

from archive.versions import compare_version_urls

from scrape import get_active_reporting_frameworks


def main() -> None:
    """Entry point for the new-URL check."""
    scraped = get_active_reporting_frameworks()
    new_urls = compare_version_urls(scraped)

    if new_urls:
        output = {v: sorted(urls) for v, urls in new_urls.items()}
        print(dumps(output, indent=2))
    else:
        print("No new URLs found.")
        sys_exit(1)


if __name__ == "__main__":
    main()
