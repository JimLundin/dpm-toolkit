"""Tests for the versions module."""

from datetime import date

import pytest
from archive.versions import (
    Version,
    compare_version_urls,
    get_source,
    get_version,
    get_version_urls,
    get_versions,
    get_versions_by_type,
    latest_version,
)

# -- Fixtures --


def _make_version(
    version_id: str,
    version: str = "4.0",
    version_type: str = "release",
    semver: str = "4.0.0",
    d: date = date(2025, 1, 1),
) -> Version:
    """Build a Version dict with sensible defaults."""
    return Version(
        id=version_id,
        version=version,
        type=version_type,  # type: ignore[typeddict-item]
        semver=semver,
        date=d,
        original={
            "url": f"https://example.com/{version_id}/original.zip",
            "checksum": "sha256:aaa",
        },
        archive={
            "url": f"https://example.com/{version_id}/archive.zip",
            "checksum": "sha256:bbb",
        },
        converted={
            "url": f"https://example.com/{version_id}/converted.zip",
            "checksum": "sha256:ccc",
        },
    )


@pytest.fixture(name="sample_versions")
def create_sample_versions() -> list[Version]:
    """Create a diverse set of test versions."""
    return [
        _make_version(
            "3.2-sample",
            version="3.2",
            version_type="sample",
            semver="3.2.0-alpha.1",
            d=date(2023, 6, 13),
        ),
        _make_version(
            "4.0-draft",
            version="4.0",
            version_type="draft",
            semver="4.0.0-beta.1",
            d=date(2024, 10, 28),
        ),
        _make_version(
            "4.0-release",
            version="4.0",
            version_type="release",
            semver="4.0.0",
            d=date(2024, 12, 19),
        ),
        _make_version(
            "4.0-errata-4",
            version="4.0",
            version_type="errata",
            semver="4.0.4",
            d=date(2025, 3, 10),
        ),
        _make_version(
            "4.1-final",
            version="4.1",
            version_type="final",
            semver="4.1.0",
            d=date(2025, 5, 28),
        ),
        _make_version(
            "4.2-hotfix",
            version="4.2",
            version_type="hotfix",
            semver="4.2.1",
            d=date(2026, 1, 13),
        ),
    ]


# -- get_source --


class TestGetSource:
    """Tests for get_source()."""

    def test_original(
        self,
        sample_versions: list[Version],
    ) -> None:
        v = sample_versions[0]
        source = get_source(v, "original")
        assert source["url"].endswith("/original.zip")

    def test_archive(
        self,
        sample_versions: list[Version],
    ) -> None:
        v = sample_versions[0]
        source = get_source(v, "archive")
        assert source["url"].endswith("/archive.zip")

    def test_converted(
        self,
        sample_versions: list[Version],
    ) -> None:
        v = sample_versions[0]
        source = get_source(v, "converted")
        assert source["url"].endswith("/converted.zip")

    def test_unknown_source_type_raises(
        self,
        sample_versions: list[Version],
    ) -> None:
        v = sample_versions[0]
        with pytest.raises(ValueError, match="Unknown source type"):
            get_source(v, "nonexistent")  # type: ignore[arg-type]


# -- get_versions --


class TestGetVersions:
    """Tests for get_versions() loading from the real TOML file."""

    def test_returns_iterable(self) -> None:
        versions = list(get_versions())
        assert len(versions) > 0

    def test_versions_have_required_keys(self) -> None:
        versions = list(get_versions())
        for v in versions:
            assert "id" in v
            assert "version" in v
            assert "type" in v
            assert "semver" in v
            assert "date" in v
            assert "original" in v

    def test_versions_types_are_valid(self) -> None:
        valid_types = {
            "sample",
            "draft",
            "final",
            "release",
            "errata",
            "hotfix",
        }
        versions = list(get_versions())
        for v in versions:
            assert v["type"] in valid_types


# -- get_versions_by_type --


class TestGetVersionsByType:
    """Tests for get_versions_by_type()."""

    def test_group_all_returns_everything(
        self,
        sample_versions: list[Version],
    ) -> None:
        result = list(get_versions_by_type(sample_versions, "all"))
        assert len(result) == 6

    def test_group_release_includes_release_errata_hotfix(
        self,
        sample_versions: list[Version],
    ) -> None:
        result = list(
            get_versions_by_type(sample_versions, "release"),
        )
        ids = {v["id"] for v in result}
        assert ids == {"4.0-release", "4.0-errata-4", "4.2-hotfix"}

    def test_group_draft_includes_draft_and_sample(
        self,
        sample_versions: list[Version],
    ) -> None:
        result = list(
            get_versions_by_type(sample_versions, "draft"),
        )
        ids = {v["id"] for v in result}
        assert ids == {"3.2-sample", "4.0-draft"}

    def test_group_release_excludes_final(
        self,
        sample_versions: list[Version],
    ) -> None:
        """'final' type is not in the 'release' group."""
        result = list(
            get_versions_by_type(sample_versions, "release"),
        )
        ids = {v["id"] for v in result}
        assert "4.1-final" not in ids

    def test_unknown_group_raises(
        self,
        sample_versions: list[Version],
    ) -> None:
        with pytest.raises(ValueError, match="Unknown version group"):
            list(get_versions_by_type(sample_versions, "invalid"))  # type: ignore[arg-type]

    def test_empty_input_returns_empty(self) -> None:
        result = list(get_versions_by_type([], "release"))
        assert result == []


# -- latest_version --


class TestLatestVersion:
    """Tests for latest_version()."""

    def test_returns_most_recent_date(
        self,
        sample_versions: list[Version],
    ) -> None:
        latest = latest_version(sample_versions)
        assert latest["id"] == "4.2-hotfix"
        assert latest["date"] == date(2026, 1, 13)

    def test_single_version(self) -> None:
        v = _make_version("only-one", d=date(2025, 6, 1))
        result = latest_version([v])
        assert result["id"] == "only-one"

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="max"):
            latest_version([])


# -- get_version --


class TestGetVersion:
    """Tests for get_version()."""

    def test_finds_existing_version(
        self,
        sample_versions: list[Version],
    ) -> None:
        result = get_version(sample_versions, "4.0-release")
        assert result is not None
        assert result["id"] == "4.0-release"

    def test_returns_none_for_missing(
        self,
        sample_versions: list[Version],
    ) -> None:
        result = get_version(sample_versions, "nonexistent")
        assert result is None

    def test_returns_first_match(self) -> None:
        """Return the first one when duplicates exist."""
        v1 = _make_version("dup", d=date(2025, 1, 1))
        v2 = _make_version("dup", d=date(2025, 6, 1))
        result = get_version([v1, v2], "dup")
        assert result is not None
        assert result["date"] == date(2025, 1, 1)

    def test_empty_versions_returns_none(self) -> None:
        result = get_version([], "anything")
        assert result is None


# -- get_version_urls --


class TestGetVersionUrls:
    """Tests for get_version_urls() using the real TOML file."""

    def test_returns_dict(self) -> None:
        urls = get_version_urls()
        assert isinstance(urls, dict)

    def test_urls_are_sets(self) -> None:
        urls = get_version_urls()
        for url_set in urls.values():
            assert isinstance(url_set, set)
            for url in url_set:
                assert url.startswith("http")


# -- compare_version_urls --


class TestCompareVersionUrls:
    """Tests for compare_version_urls()."""

    def test_known_url_is_not_new(self) -> None:
        """Existing versions.toml URLs are not flagged."""
        existing_urls = get_version_urls()
        result = compare_version_urls(existing_urls)
        assert result == {}

    def test_new_url_is_detected(self) -> None:
        """Brand new URL should be reported."""
        new_urls = {
            "5.0": {"https://example.com/brand-new.zip"},
        }
        result = compare_version_urls(new_urls)
        assert "5.0" in result
        assert "https://example.com/brand-new.zip" in result["5.0"]

    def test_mix_of_new_and_existing(self) -> None:
        """Only truly new URLs should be returned."""
        existing = get_version_urls()
        first_version = next(iter(existing))
        known_url = next(iter(existing[first_version]))

        mixed = {
            first_version: {
                known_url,
                "https://example.com/new.zip",
            },
        }
        result = compare_version_urls(mixed)
        if first_version in result:
            assert known_url not in result[first_version]
            assert "https://example.com/new.zip" in result[first_version]

    def test_empty_input_returns_empty(self) -> None:
        result = compare_version_urls({})
        assert result == {}
