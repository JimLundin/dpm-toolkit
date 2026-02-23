"""Tests for the download module."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from archive.download import download_source, verify_checksum

if TYPE_CHECKING:
    from archive.versions import Source


# -- verify_checksum --


class TestVerifyChecksum:
    """Tests for verify_checksum()."""

    def test_valid_sha256_checksum(self) -> None:
        data = b"hello world"
        expected = f"sha256:{sha256(data).hexdigest()}"
        assert verify_checksum(data, expected) is True

    def test_invalid_sha256_checksum(self) -> None:
        data = b"hello world"
        bad = "sha256:0000000000000000"
        assert verify_checksum(data, bad) is False

    def test_non_sha256_prefix_returns_false(self) -> None:
        data = b"hello world"
        assert verify_checksum(data, "md5:abc123") is False

    def test_empty_data(self) -> None:
        data = b""
        expected = f"sha256:{sha256(data).hexdigest()}"
        assert verify_checksum(data, expected) is True

    def test_large_data(self) -> None:
        data = b"x" * 10_000_000
        expected = f"sha256:{sha256(data).hexdigest()}"
        assert verify_checksum(data, expected) is True

    def test_bare_hash_without_prefix_returns_false(self) -> None:
        data = b"hello"
        bare_hash = sha256(data).hexdigest()
        assert verify_checksum(data, bare_hash) is False


# -- download_source --


class TestDownloadSource:
    """Tests for download_source() with mocked HTTP."""

    @patch("archive.download.get")
    def test_successful_download(
        self,
        mock_get: MagicMock,
    ) -> None:
        content = b"database content"
        checksum = f"sha256:{sha256(content).hexdigest()}"

        mock_response = MagicMock()
        mock_response.content = content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        source: Source = {
            "url": "https://example.com/db.zip",
            "checksum": checksum,
        }
        result = download_source(source)

        assert isinstance(result, BytesIO)
        assert result.read() == content
        mock_get.assert_called_once()

    @patch("archive.download.get")
    def test_failed_checksum_still_returns_data(
        self,
        mock_get: MagicMock,
    ) -> None:
        """Data is returned even if checksum fails."""
        content = b"database content"

        mock_response = MagicMock()
        mock_response.content = content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        source: Source = {
            "url": "https://example.com/db.zip",
            "checksum": "sha256:wrong",
        }
        result = download_source(source)

        assert isinstance(result, BytesIO)
        assert result.read() == content

    @patch("archive.download.get")
    def test_raises_on_http_error(
        self,
        mock_get: MagicMock,
    ) -> None:
        """HTTP errors should propagate."""
        from requests.exceptions import HTTPError

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError(
            "404 Not Found",
        )
        mock_get.return_value = mock_response

        source: Source = {
            "url": "https://example.com/db.zip",
            "checksum": "sha256:abc",
        }
        with pytest.raises(HTTPError):
            download_source(source)

    @patch("archive.download.get")
    def test_sets_user_agent_header(
        self,
        mock_get: MagicMock,
    ) -> None:
        """Verify User-Agent header is set."""
        mock_response = MagicMock()
        mock_response.content = b"data"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        source: Source = {
            "url": "https://example.com/db.zip",
            "checksum": "sha256:abc",
        }
        download_source(source)

        call_kwargs = mock_get.call_args.kwargs
        assert "headers" in call_kwargs
        assert "User-Agent" in call_kwargs["headers"]
        assert "Mozilla" in call_kwargs["headers"]["User-Agent"]
