"""Tests that the installed dpm2 package is structurally complete.

These tests verify the packaging itself — that the built wheel includes
all the files it's supposed to.  They run only when dpm2 is installed
from a wheel (not from an editable source tree), because the source
tree legitimately doesn't have ``dpm.sqlite`` (it's generated during
the build).

When dpm2 IS installed from a wheel and the database is missing,
these tests fail loudly — exactly the scenario we want to catch.
"""

from __future__ import annotations

import dpm2
import pytest
from dpm2.utils import get_source_db_resource

SQLITE_MAGIC = b"SQLite format 3\x00"

# dpm2 is installed from a wheel when its __file__ is inside site-packages.
# In an editable/source install it lives in projects/dpm2/src/dpm2/.
_installed_from_wheel = "site-packages" in (dpm2.__file__ or "")

requires_wheel_install = pytest.mark.skipif(
    not _installed_from_wheel,
    reason="dpm2 installed from source tree; wheel-only packaging checks skipped",
)

pytestmark = requires_wheel_install


class TestPackageContents:
    """The built wheel must include the bundled database."""

    def test_dpm_sqlite_is_included(self) -> None:
        """``dpm.sqlite`` must exist as a package resource."""
        resource = get_source_db_resource()
        with resource.open("rb") as fh:
            header = fh.read(len(SQLITE_MAGIC))
        assert header == SQLITE_MAGIC, (
            f"dpm.sqlite is missing or not a valid SQLite file "
            f"(header: {header!r})"
        )

    def test_dpm_sqlite_is_non_empty(self) -> None:
        """``dpm.sqlite`` must contain more than just the SQLite header."""
        resource = get_source_db_resource()
        with resource.open("rb") as fh:
            data = fh.read()
        # A non-empty SQLite file is at least one page (default 4096 bytes).
        min_size = 4096
        assert len(data) >= min_size, (
            f"dpm.sqlite is suspiciously small ({len(data)} bytes)"
        )
