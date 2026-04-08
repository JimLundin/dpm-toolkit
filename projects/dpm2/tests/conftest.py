from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from dpm2.models import DPM
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(name="empty_engine", scope="session")
def create_empty_engine() -> Iterator[Engine]:
    """Create an in-memory database with the full DPM schema but no data."""
    engine = create_engine("sqlite:///:memory:")
    DPM.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture(name="empty_session")
def create_empty_session(empty_engine: Engine) -> Iterator[Session]:
    """Session bound to the empty schema database."""
    with Session(empty_engine) as session:
        yield session


def _bundled_db_available() -> bool:
    """Check whether the bundled dpm.sqlite ships with this install."""
    try:
        from dpm2.utils import get_source_db_resource

        resource = get_source_db_resource()
        # Traversable has no .exists(); try to open it instead.
        resource.open("rb").close()
    except Exception:  # noqa: BLE001
        return False
    return True


has_bundled_db = _bundled_db_available()

requires_db = pytest.mark.skipif(
    not has_bundled_db,
    reason="Bundled dpm.sqlite not available (generated at release time)",
)


@pytest.fixture(name="db_engine", scope="session")
def create_db_engine() -> Iterator[Engine]:
    """Engine connected to the real bundled DPM database (in-memory copy)."""
    if not has_bundled_db:
        pytest.skip("Bundled dpm.sqlite not available")
    from dpm2 import get_db

    engine = get_db(in_memory=True)
    yield engine
    engine.dispose()


@pytest.fixture(name="db_session")
def create_db_session(db_engine: Engine) -> Iterator[Session]:
    """Read-only session against the real bundled database."""
    with Session(db_engine) as session:
        yield session
