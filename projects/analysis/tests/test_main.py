"""Tests for main analysis module."""

from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from analysis.main import create_engine_for_database


def test_engine_has_connection_pooling() -> None:
    """Test that created engines have connection pooling configured."""
    # Create a temporary SQLite database
    with NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        engine = create_engine_for_database(tmp_path)

        # Verify connection pooling is configured
        assert engine.pool is not None
        assert engine.pool.size() == 5  # pool_size
        assert engine.pool._max_overflow == 10  # max_overflow  # noqa: SLF001

        # Verify pre_ping is enabled (harder to test directly, but config accepted)
        # Pre-ping means SQLAlchemy will test connections before using them

    finally:
        # Clean up
        tmp_path.unlink()


def test_engine_for_different_database_types() -> None:
    """Test that engines are created for different database file types."""
    test_cases = [
        (".sqlite", "sqlite"),
        (".db", "sqlite"),
        (".sqlite3", "sqlite"),
    ]

    for suffix, expected_dialect in test_cases:
        with NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            engine = create_engine_for_database(tmp_path)

            # Verify correct dialect
            assert engine.dialect.name == expected_dialect

            # Verify pooling configured
            assert engine.pool is not None

        finally:
            tmp_path.unlink()


def test_unsupported_database_extension() -> None:
    """Test that unsupported extensions raise ValueError."""
    with NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with pytest.raises(ValueError, match="Unsupported database extension"):
            create_engine_for_database(tmp_path)
    finally:
        tmp_path.unlink()
