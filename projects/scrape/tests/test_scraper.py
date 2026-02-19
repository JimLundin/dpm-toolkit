"""Tests for the scrape module."""

from unittest.mock import MagicMock, patch

import pytest
from scrape.scraper import (
    _is_dpm2_database,
    _parse_version,
    get_active_reporting_frameworks,
    get_dpm_urls,
    get_framework_urls,
)

# ---------------------------------------------------------------------------
# _parse_version
# ---------------------------------------------------------------------------


class TestParseVersion:
    def test_two_digits(self) -> None:
        assert _parse_version("42") == "4.2"

    def test_three_digits(self) -> None:
        assert _parse_version("210") == "2.10"

    def test_single_digit(self) -> None:
        assert _parse_version("4") == "4"

    def test_two_digit_minor(self) -> None:
        assert _parse_version("35") == "3.5"


# ---------------------------------------------------------------------------
# _is_dpm2_database
# ---------------------------------------------------------------------------


class TestIsDpm2Database:
    """Test DPM 2.0 database URL/text detection."""

    # --- positive cases ---

    @pytest.mark.parametrize(
        ("href", "text"),
        [
            # URL-encoded DPM2 filename (4.2 release)
            (
                "https://www.eba.europa.eu/sites/default/files/2025-11/"
                "d67068fe/DPM2%20Database_v%204_2_20251125.zip",
                "DPM database 2.0",
            ),
            # Underscore-separated DPM 2.0 (4.0 draft)
            (
                "https://www.eba.europa.eu/sites/default/files/2024-10/"
                "9af4a241/dpm_2.0_release_4.0.zip",
                "",
            ),
            # DPM2.0 filename (4.0 errata)
            (
                "https://www.eba.europa.eu/sites/default/files/2025-03/"
                "c235b72f/DPM2.0_release_4.0-2025-03-10.zip",
                "",
            ),
            # Space-separated DPM 2.0 (4.1 final)
            (
                "https://www.eba.europa.eu/sites/default/files/2025-05/"
                "b0fb26ab/DPM%202.0%20release%204.1.zip",
                "",
            ),
            # Text-only match (generic filename)
            (
                "https://www.eba.europa.eu/sites/default/files/some_database.zip",
                "DPM database 2.0",
            ),
            # Sample DPM 2.0 database (3.2)
            (
                "https://www.eba.europa.eu/assets/Reporting%20Framework/"
                "sample_dpm_2.0_database.zip",
                "",
            ),
            # DPM2 without dot (4.2 draft)
            (
                "https://www.eba.europa.eu/sites/default/files/2025-09/"
                "2e9220cc/DPM2%20Database_Release%204.2%28draft%29.accdb_.zip",
                "",
            ),
        ],
    )
    def test_matches_dpm2(self, href: str, text: str) -> None:
        assert _is_dpm2_database(href, text)

    # --- negative cases ---

    @pytest.mark.parametrize(
        ("href", "text"),
        [
            # Non-zip file
            (
                "https://www.eba.europa.eu/sites/default/files/glossary.xlsx",
                "DPM 2.0",
            ),
            # Glossary zip (excluded)
            (
                "https://www.eba.europa.eu/sites/default/files/dpm2_glossary.zip",
                "",
            ),
            # Conversion file (excluded even though it contains dpm_2)
            (
                "https://www.eba.europa.eu/sites/default/files/"
                "conversion_file_between_dpm_1.0_and_dpm_2.0_glossary.zip",
                "",
            ),
            # DPM 1.0 database — positive pattern doesn't match
            (
                "https://www.eba.europa.eu/sites/default/files/DPM1.0_release_4.0.zip",
                "DPM database 1.0",
            ),
            # Unrelated zip
            (
                "https://www.eba.europa.eu/sites/default/files/"
                "ITS%20on%20supervisory%20reporting.zip",
                "ITS on supervisory reporting",
            ),
            # Table layout (excluded)
            (
                "https://www.eba.europa.eu/sites/default/files/dpm2_table_layout.zip",
                "",
            ),
        ],
    )
    def test_rejects_non_dpm2(self, href: str, text: str) -> None:
        assert not _is_dpm2_database(href, text)


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

FRAMEWORKS_HTML = """\
<html><body>
<a href="/risk-and-data-analysis/reporting/reporting-frameworks/\
reporting-framework-43">Framework 4.3</a>
<a href="/risk-and-data-analysis/reporting-frameworks/\
reporting-framework-42">Framework 4.2</a>
<a href="/risk-and-data-analysis/reporting-frameworks/\
reporting-framework-41">Framework 4.1</a>
<a href="/risk-and-data-analysis/reporting/reporting-frameworks/\
reporting-framework-40">Framework 4.0</a>
<a href="/risk-and-data-analysis/reporting/reporting-frameworks/\
reporting-framework-35">Framework 3.5</a>
<a href="/risk-and-data-analysis/reporting/reporting-frameworks/\
reporting-framework-20">Framework 2.0</a>
<a href="/other/page">Unrelated link</a>
</body></html>
"""

FRAMEWORK_PAGE_HTML = """\
<html><body>
<a href="/sites/default/files/2025-11/abc123/\
DPM2%20Database_v%204_2.zip">DPM database 2.0</a>
<a href="/sites/default/files/2025-07/def456/\
DPM%201.0%20Database.zip">DPM database 1.0</a>
<a href="/sites/default/files/2025-07/ghi789/Glossary.xlsx">\
DPM dictionary</a>
<a href="/sites/default/files/2025-02/jkl012/\
ITS%20on%20reporting.zip">ITS on supervisory reporting</a>
</body></html>
"""

EMPTY_PAGE_HTML = """\
<html><body>
<a href="/some/page">No DPM links here</a>
</body></html>
"""


def _mock_response(html: str) -> MagicMock:
    """Create a mock requests response."""
    mock = MagicMock()
    mock.text = html
    mock.status_code = 200
    mock.raise_for_status = MagicMock()
    return mock


# ---------------------------------------------------------------------------
# get_framework_urls
# ---------------------------------------------------------------------------


class TestGetFrameworkUrls:
    @patch("scrape.scraper._create_session")
    def test_finds_all_versions(self, mock_create: MagicMock) -> None:
        session = MagicMock()
        session.get.return_value = _mock_response(FRAMEWORKS_HTML)
        mock_create.return_value = session

        result = get_framework_urls()

        assert "4.3" in result
        assert "4.2" in result
        assert "4.1" in result
        assert "4.0" in result
        assert "3.5" in result
        assert "2.0" in result

    @patch("scrape.scraper._create_session")
    def test_urls_are_absolute(self, mock_create: MagicMock) -> None:
        session = MagicMock()
        session.get.return_value = _mock_response(FRAMEWORKS_HTML)
        mock_create.return_value = session

        result = get_framework_urls()

        for url in result.values():
            assert url.startswith("https://")

    @patch("scrape.scraper._create_session")
    def test_ignores_unrelated_links(self, mock_create: MagicMock) -> None:
        session = MagicMock()
        session.get.return_value = _mock_response(FRAMEWORKS_HTML)
        mock_create.return_value = session

        result = get_framework_urls()

        # Only framework links should be present
        assert len(result) == 6

    def test_accepts_explicit_session(self) -> None:
        session = MagicMock()
        session.get.return_value = _mock_response(FRAMEWORKS_HTML)

        result = get_framework_urls(session)

        assert len(result) == 6


# ---------------------------------------------------------------------------
# get_dpm_urls
# ---------------------------------------------------------------------------


class TestGetDpmUrls:
    def test_finds_dpm2_database(self) -> None:
        session = MagicMock()
        session.get.return_value = _mock_response(FRAMEWORK_PAGE_HTML)

        result = get_dpm_urls(session, "https://www.eba.europa.eu/framework")

        assert len(result) == 1

    def test_excludes_dpm1(self) -> None:
        session = MagicMock()
        session.get.return_value = _mock_response(FRAMEWORK_PAGE_HTML)

        result = get_dpm_urls(session, "https://www.eba.europa.eu/framework")

        for url in result:
            assert "DPM%201.0" not in url

    def test_excludes_non_database_zips(self) -> None:
        session = MagicMock()
        session.get.return_value = _mock_response(FRAMEWORK_PAGE_HTML)

        result = get_dpm_urls(session, "https://www.eba.europa.eu/framework")

        for url in result:
            assert "ITS" not in url

    def test_empty_page_returns_empty_set(self) -> None:
        session = MagicMock()
        session.get.return_value = _mock_response(EMPTY_PAGE_HTML)

        result = get_dpm_urls(session, "https://www.eba.europa.eu/framework")

        assert result == set()


# ---------------------------------------------------------------------------
# get_active_reporting_frameworks
# ---------------------------------------------------------------------------


class TestGetActiveReportingFrameworks:
    @patch("scrape.scraper._create_session")
    def test_filters_old_versions(self, mock_create: MagicMock) -> None:
        session = MagicMock()
        session.get.side_effect = [
            _mock_response(FRAMEWORKS_HTML),  # main page
            _mock_response(FRAMEWORK_PAGE_HTML),  # 3.5
            _mock_response(FRAMEWORK_PAGE_HTML),  # 4.0
            _mock_response(FRAMEWORK_PAGE_HTML),  # 4.1
            _mock_response(FRAMEWORK_PAGE_HTML),  # 4.2
            _mock_response(FRAMEWORK_PAGE_HTML),  # 4.3
        ]
        mock_create.return_value = session

        result = get_active_reporting_frameworks()

        # Versions < 3.0 should be excluded
        assert "2.0" not in result

    @patch("scrape.scraper._create_session")
    def test_skips_pages_without_dpm(self, mock_create: MagicMock) -> None:
        session = MagicMock()
        session.get.side_effect = [
            _mock_response(FRAMEWORKS_HTML),  # main page
            _mock_response(FRAMEWORK_PAGE_HTML),  # 3.5 — has DPM
            _mock_response(EMPTY_PAGE_HTML),  # 4.0 — no DPM
            _mock_response(FRAMEWORK_PAGE_HTML),  # 4.1 — has DPM
            _mock_response(EMPTY_PAGE_HTML),  # 4.2 — no DPM
            _mock_response(EMPTY_PAGE_HTML),  # 4.3 — no DPM
        ]
        mock_create.return_value = session

        result = get_active_reporting_frameworks()

        assert "4.0" not in result
        assert "4.2" not in result
        # 3.5 and 4.1 should be present
        assert "3.5" in result
        assert "4.1" in result

    @patch("scrape.scraper._create_session")
    def test_handles_request_errors_gracefully(
        self,
        mock_create: MagicMock,
    ) -> None:
        from requests.exceptions import ConnectionError as ReqConnectionError

        session = MagicMock()
        session.get.side_effect = [
            _mock_response(FRAMEWORKS_HTML),  # main page
            ReqConnectionError("timeout"),  # 3.5 fails
            _mock_response(FRAMEWORK_PAGE_HTML),  # 4.0
            _mock_response(FRAMEWORK_PAGE_HTML),  # 4.1
            _mock_response(FRAMEWORK_PAGE_HTML),  # 4.2
            _mock_response(FRAMEWORK_PAGE_HTML),  # 4.3
        ]
        mock_create.return_value = session

        result = get_active_reporting_frameworks()

        # Should still return results for pages that succeeded
        assert "3.5" not in result
        assert "4.0" in result
