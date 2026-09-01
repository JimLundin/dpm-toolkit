"""db utilities for using the dpm db."""

import lzma
import os
import shutil
import sys
import tempfile
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import as_file, files
from importlib.resources.abc import Traversable
from pathlib import Path
from sqlite3 import connect

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.event import listen
from sqlalchemy.pool import ConnectionPoolEntry

DB_NAME = "dpm.sqlite"
COMPRESSED_DB_NAME = f"{DB_NAME}.xz"
COPY_CHUNK_SIZE = 1 << 22


def get_source_db_resource() -> Traversable:
    """Get the bundled SQLite database, preferring an uncompressed copy.

    Published wheels ship the database LZMA-compressed to stay within PyPI's
    per-file size limit, so ``dpm.sqlite.xz`` is the usual shape.  A plain
    ``dpm.sqlite`` still wins when present, which keeps local development
    against a freshly converted database working unchanged.
    """
    package_files = files("dpm2")
    plain_db = package_files / DB_NAME
    if plain_db.is_file():
        return plain_db
    return package_files / COMPRESSED_DB_NAME


def package_version() -> str:
    """Version of the installed dpm2 distribution, or ``unknown``."""
    try:
        return version("dpm2")
    except PackageNotFoundError:
        return "unknown"


def cache_root() -> Path:
    """Locate the base directory for cached copies of the database.

    Honours ``DPM2_CACHE_DIR`` first, then the platform convention:
    ``%LOCALAPPDATA%`` on Windows, ``$XDG_CACHE_HOME`` (default ``~/.cache``)
    elsewhere.
    """
    if override := os.environ.get("DPM2_CACHE_DIR"):
        return Path(override)

    if sys.platform == "win32":
        local_app_data = os.environ.get("LOCALAPPDATA")
        base = Path(local_app_data) if local_app_data else Path.home() / "AppData"
        return base / "dpm2" / "cache"

    xdg_cache = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg_cache) if xdg_cache else Path.home() / ".cache"
    return base / "dpm2"


def cached_db_path() -> Path:
    """Path of the decompressed database for the installed dpm2 version.

    The version is part of the path so upgrading dpm2 never reuses the
    database cached by a previous release.
    """
    return cache_root() / package_version() / DB_NAME


def extract_db(source: Traversable, target: Path) -> None:
    """Decompress *source* to *target*, atomically.

    The stream is written to a sibling temporary file and moved into place
    only once it is complete, so a crashed or concurrent extraction can never
    leave a half-written database behind for the next process to open.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    handle, temp_name = tempfile.mkstemp(dir=target.parent, suffix=".partial")
    temp_path = Path(temp_name)
    try:
        with (
            source.open("rb") as raw,
            lzma.open(raw) as compressed,
            os.fdopen(handle, "wb") as partial,
        ):
            shutil.copyfileobj(compressed, partial, COPY_CHUNK_SIZE)
        temp_path.replace(target)
    finally:
        # A successful replace already moved the file, so this is a no-op then
        # and a cleanup when the copy failed part way through.
        temp_path.unlink(missing_ok=True)


def ensure_local_db() -> Path:
    """Return an on-disk path to the bundled database.

    For a compressed distribution the first call decompresses the database
    into the user cache directory (a few seconds, roughly 300 MB on disk);
    later calls reuse that copy.
    """
    db_resource = get_source_db_resource()

    if db_resource.name == DB_NAME:
        with as_file(db_resource) as db_path:
            if not db_path.exists():
                msg = f"Database file not found: {db_path}"
                raise FileNotFoundError(msg)
            return db_path.resolve()

    cached_db = cached_db_path()
    if not cached_db.exists():
        if not db_resource.is_file():
            msg = f"Database file not found: {db_resource}"
            raise FileNotFoundError(msg)
        extract_db(db_resource, cached_db)

    return cached_db


def set_readonly(connection: DBAPIConnection, _record: ConnectionPoolEntry) -> None:
    """Set the connection to readonly.

    The ``connect`` event hands over the raw DBAPI connection, so the
    statement goes through ``sqlite3`` as plain SQL rather than through
    SQLAlchemy's ``text()``.
    """
    connection.cursor().execute("PRAGMA query_only = true")


def disk_engine(db_path: Path) -> Engine:
    """Create a read-only engine connected to a SQLite file on disk."""
    engine = create_engine(
        f"sqlite:///{db_path}?mode=ro",
        connect_args={"uri": True},
    )
    listen(engine, "connect", set_readonly)
    return engine


def in_memory_engine(db_path: Path) -> Engine:
    """Load a SQLite file into memory and return an engine to the copy."""
    memory_db = connect(":memory:")
    with connect(db_path) as source_db:
        source_db.backup(memory_db)
    return create_engine("sqlite://", creator=lambda: memory_db)


def get_db(*, in_memory: bool = True) -> Engine:
    """Get an engine to the bundled DPM database.

    When *in_memory* is ``True`` (default) the database is copied into
    memory inside the ``as_file`` context so the temporary extraction
    path can be safely cleaned up afterwards.

    When *in_memory* is ``False`` the path is resolved to an absolute
    path before the context manager exits.  This works reliably for
    regular installs (where the resource lives on disk) but may break
    for zip-imported packages that extract to a temporary directory.

    A compressed distribution has neither concern: the database is
    decompressed once into a real cache file (see :func:`ensure_local_db`)
    that outlives the call.
    """
    db_resource = get_source_db_resource()

    if db_resource.name == COMPRESSED_DB_NAME:
        cached_db = ensure_local_db()
        if in_memory:
            return in_memory_engine(cached_db)
        return disk_engine(cached_db)

    with as_file(db_resource) as db_path:
        if not db_path.exists():
            msg = f"Database file not found: {db_path}"
            raise FileNotFoundError(msg)

        if in_memory:
            return in_memory_engine(db_path)
        resolved = db_path.resolve()

    return disk_engine(resolved)
