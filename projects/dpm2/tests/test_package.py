"""Tests that the installed dpm2 package is structurally complete.

These tests verify the packaging itself — that the built wheel includes
all the files it's supposed to.  They run only when dpm2 is installed
from a wheel (not from an editable source tree), because the source
tree legitimately doesn't have a bundled database (it's generated
during the build).

When dpm2 IS installed from a wheel and the database is missing,
these tests fail loudly — exactly the scenario we want to catch.
"""

from __future__ import annotations

import pytest

import dpm2
from dpm2.utils import COMPRESSED_DB_NAME, ensure_local_db, get_source_db_resource

SQLITE_MAGIC = b"SQLite format 3\x00"
XZ_MAGIC = b"\xfd7zXZ\x00"

# dpm2 is installed from a wheel when its __file__ is inside site-packages.
# In an editable/source install it lives in projects/dpm2/src/dpm2/.
_installed_from_wheel = "site-packages" in (dpm2.__file__ or "")

requires_wheel_install = pytest.mark.skipif(
    not _installed_from_wheel,
    reason="dpm2 installed from source tree; wheel-only packaging checks skipped",
)

pytestmark = requires_wheel_install


class TestPackageContents:
    """The built wheel must include a usable bundled database."""

    def test_bundled_database_is_included(self) -> None:
        """The database must exist as a package resource.

        Published wheels ship it LZMA-compressed to stay within PyPI's
        per-file size limit, so either shape is acceptable as long as the
        magic bytes match the name.
        """
        resource = get_source_db_resource()
        with resource.open("rb") as fh:
            header = fh.read(len(SQLITE_MAGIC))

        if resource.name == COMPRESSED_DB_NAME:
            assert header.startswith(XZ_MAGIC), (
                f"{resource.name} is not a valid xz file (header: {header!r})"
            )
        else:
            assert header == SQLITE_MAGIC, (
                f"{resource.name} is not a valid SQLite file (header: {header!r})"
            )

    def test_bundled_database_is_non_empty(self) -> None:
        """The bundled database must be more than an empty header.

        Measured after decompression: the size of a compressed blob says
        nothing useful about the database inside it.
        """
        db_path = ensure_local_db()
        size = db_path.stat().st_size
        # A non-empty SQLite file is at least one page (default 4096 bytes).
        min_size = 4096
        assert size >= min_size, f"{db_path} is suspiciously small ({size} bytes)"

    def test_bundled_database_resolves_to_sqlite(self) -> None:
        """A compressed database must decompress to a real SQLite file.

        This is what actually protects users: the wheel can carry a
        perfectly valid xz archive and still be useless if what comes out
        of it is not a database.
        """
        db_path = ensure_local_db()
        with db_path.open("rb") as fh:
            header = fh.read(len(SQLITE_MAGIC))
        assert header == SQLITE_MAGIC, (
            f"{db_path} is not a valid SQLite file (header: {header!r})"
        )
