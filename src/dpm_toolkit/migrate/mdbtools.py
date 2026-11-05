"""Read a Microsoft Access database through GNU mdbtools.

Cross-platform counterpart to the ``access+pyodbc`` reader in
:mod:`dpm_toolkit.migrate.processing`, which needs the Microsoft Access ODBC
driver and therefore Windows. mdbtools is packaged for Debian/Ubuntu
(``mdbtools``) and Homebrew, so this path runs on Linux and macOS too.

No single mdbtools output carries everything a ``MetaData`` needs, so the
reader assembles one from three:

* ``mdb-schema <db> sqlite`` — tables, columns and primary keys. Loaded into a
  throwaway SQLite file and reflected, so SQLAlchemy does the DDL parsing
  rather than a regex here.
* ``mdb-schema <db> access`` — native Access types and their lengths. The
  sqlite backend flattens ``Replication ID`` to ``INTEGER`` and drops
  ``VARCHAR`` lengths, so types are overlaid from this output. Only the
  per-column type is read from it; the column set itself comes from
  reflection.
* ``MSysRelationships`` — declared relationships. Read directly rather than via
  ``mdb-schema --relations`` because that omits the ones Access marks "do not
  enforce", and the DPM schema has 11 of them.

Row data comes from ``mdb-export``, which emits text, so values are coerced
back to Python types using the overlaid column types.

One fidelity loss is irreducible: mdbtools reports Access's zero-length strings
as NULL, so in a *nullable* text column ``''`` and NULL cannot be told apart.
Where Access declares the column NOT NULL the ambiguity resolves — the value
cannot have been NULL — and :func:`_coerce` restores the empty string. Where it
does not, the empty string is read as NULL: about 2,950 cells across six
``Description`` columns in DPM 4.4. Both spellings mean "no description", but
the distinction is not recoverable from mdbtools output.

mdbtools 1.0.0 or newer is required. 0.7.x cannot open ``.accdb`` files at all
and lacks ``--null``, without which NULL and the empty string are
indistinguishable in the exported CSV.
"""

from __future__ import annotations

import csv
import re
from datetime import datetime
from io import StringIO
from logging import getLogger
from pathlib import Path
from shutil import which
from subprocess import run
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import (
    CHAR,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    create_engine,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy import Column, Table
    from sqlalchemy.types import TypeEngine

    from .transformations import Rows

logger = getLogger(__name__)

MINIMUM_VERSION: Final = (1, 0, 0)
"""Oldest mdbtools that can open ``.accdb`` and supports ``--null``."""

NULL: Final = r"\N"
"""Sentinel passed to ``mdb-export --null``; emitted unquoted, so it cannot
collide with a genuine empty string, which is emitted as ``""``."""

DATETIME_FORMAT: Final = "%Y-%m-%d %H:%M:%S"

POISON_DATE: Final = "1900-01-00"
"""What mdbtools emits for a date outside its supported range.

``mdb_date_to_tm`` (``src/libmdb/data.c``) returns early without writing for
``td < 0.0 || td > 1e6``, leaving a zeroed ``struct tm`` that ``strftime``
renders as ``1900-01-00``. Day 00 is impossible, so detection never
false-positives on real data, but the true value is unrecoverable — both the
far-future and pre-1900 branches produce it. Present in current upstream.
"""

FAR_FUTURE: Final = datetime(9999, 12, 31)  # noqa: DTZ001
"""What :data:`POISON_DATE` is repaired to.

DPM uses ``9999-12-31`` as its open-ended sentinel. Verified against a
pyodbc-derived database for both affected columns: ``ModuleVersion``
``.FromReferenceDate`` and ``OperationScope.FromSubmissionDate`` hold
``9999-12-31`` there, in the same rows mdbtools poisons.
"""

POISON_ALLOWLIST: Final = frozenset(
    {
        ("ModuleVersion", "FromReferenceDate"),
        ("OperationScope", "FromSubmissionDate"),
    },
)
"""Columns known to hold far-future dates, so known to be safe to repair.

Row counts are deliberately not pinned — they grow legitimately with each
release. A poisoned value in any column *outside* this set is the real risk,
because repairing it would invent a wrong date, so that fails the conversion.
"""

_SCALE: Final = 1
"""Index of the scale value in an Access `Numeric (precision, scale)` size."""

_GUID_DIGITS: Final = 32
"""Hex digits in a replication ID, before canonical hyphens are inserted."""

_SYSTEM_TABLE_PREFIXES: Final = ("MSys", "~")
"""Prefixes of Access-internal tables that are not part of the data model."""

_ACCESS_TYPES: Final[Mapping[str, Callable[[tuple[int, ...]], TypeEngine[Any]]]] = {
    "Boolean": lambda _: Boolean(),
    "Byte": lambda _: Integer(),
    "Integer": lambda _: Integer(),
    "Long Integer": lambda _: Integer(),
    "Single": lambda _: Float(),
    "Double": lambda _: Float(),
    "DateTime": lambda _: DateTime(),
    "Text": lambda spec: String(spec[0] if spec else None),
    # Access reports Numeric as `Numeric (precision, scale)`.
    "Numeric": lambda spec: Numeric(
        precision=spec[0] if spec else None,
        scale=spec[1] if len(spec) > _SCALE else None,
    ),
    # mdbtools reports Access's declared size for memo columns too, but the
    # pyodbc path discards it and maps them to unbounded text, so the size is
    # dropped here to match. Honouring it diverges on 15 columns.
    "Memo/Hyperlink": lambda _: Text(),
    # Stored in the canonical 8-4-4-4-12 form, so 36 characters.
    "Replication ID": lambda _: CHAR(36),
}

_TABLE_LINE = re.compile(r"^CREATE TABLE \[(?P<table>[^\]]+)\]")
# The size specifier holds one value for `Text (255)` and two for
# `Numeric (19, 0)`, so it is captured whole and split.
_COLUMN_LINE = re.compile(
    r"^\s+\[(?P<column>[^\]]+)\]\s+"
    r"(?P<type>[A-Za-z /]+?)"
    r"(?: \((?P<spec>[\d, ]*)\))?"
    r"(?: NOT NULL)?,?\s*$",
)
_COLUMN_START = re.compile(r"^\s+\[")
_NON_HEX = re.compile(r"[^0-9a-f]")


class MdbtoolsError(Exception):
    """Raised when mdbtools is unusable or returns something unexpected."""


def _binary(name: str) -> str:
    """Absolute path to an mdbtools executable.

    Args:
        name: Tool name, e.g. ``mdb-export``.

    Returns:
        Absolute path to the executable.

    Raises:
        MdbtoolsError: If the tool is not on ``PATH``.

    """
    if found := which(name):
        return found

    msg = f"{name} not found on PATH; install mdbtools (>= 1.0.0)"
    raise MdbtoolsError(msg)


def _run(name: str, *args: str) -> str:
    """Run an mdbtools command and return its stdout.

    Args:
        name: Tool name, e.g. ``mdb-schema``.
        args: Arguments passed to the tool.

    Returns:
        Standard output, decoded as UTF-8 with replacement.

    Raises:
        MdbtoolsError: If the tool exits non-zero.

    """
    # Captured as bytes and decoded here rather than with `text=True`, which
    # enables universal newlines and would rewrite the CRLF line endings inside
    # Access memo fields to LF before the CSV reader ever sees them.
    result = run(  # noqa: S603
        [_binary(name), *args],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        msg = f"{name} failed ({result.returncode}): {stderr}"
        raise MdbtoolsError(msg)

    return result.stdout.decode("utf-8", errors="replace")


def version() -> tuple[int, ...]:
    """Installed mdbtools version.

    Returns:
        Version as a tuple, e.g. ``(1, 0, 1)``.

    Raises:
        MdbtoolsError: If the version cannot be parsed. 0.7.x rejects
            ``--version`` outright, which surfaces here.

    """
    reported = _run("mdb-export", "--version").strip()
    if not (match := re.search(r"(\d+)\.(\d+)(?:\.(\d+))?", reported)):
        msg = f"could not parse mdbtools version from {reported!r}"
        raise MdbtoolsError(msg)

    return tuple(int(part) for part in match.groups(default="0"))


def available() -> bool:
    """Whether a usable mdbtools is installed.

    Returns:
        True if mdbtools is present and at least :data:`MINIMUM_VERSION`.

    """
    try:
        return version() >= MINIMUM_VERSION
    except MdbtoolsError:
        return False


def require() -> None:
    """Assert that a usable mdbtools is installed.

    Raises:
        MdbtoolsError: If mdbtools is missing or older than
            :data:`MINIMUM_VERSION`.

    """
    installed = version()
    if installed < MINIMUM_VERSION:
        wanted = ".".join(str(part) for part in MINIMUM_VERSION)
        found = ".".join(str(part) for part in installed)
        msg = f"mdbtools {wanted} or newer required, found {found}"
        raise MdbtoolsError(msg)


def _native_types(
    access_location: Path,
) -> dict[tuple[str, str], TypeEngine[Any]]:
    """Map each column to the type its native Access type implies.

    Read from ``mdb-schema``'s ``access`` backend, which preserves
    ``Replication ID`` and ``Text (n)`` where the ``sqlite`` backend flattens
    both.

    Args:
        access_location: Path to the ``.accdb`` or ``.mdb`` file.

    Returns:
        ``(table, column)`` to SQLAlchemy type.

    Raises:
        MdbtoolsError: If an Access type has no mapping. Failing loudly here
            is deliberate: a silently mistyped column would propagate into the
            published models.

    """
    ddl = _run("mdb-schema", "--not-null", str(access_location), "access")

    types: dict[tuple[str, str], TypeEngine[Any]] = {}
    table = ""
    for line in ddl.splitlines():
        if header := _TABLE_LINE.match(line):
            table = header["table"]
            continue

        if not table:
            continue

        column = _COLUMN_LINE.match(line)
        if column is None:
            # A column line that does not parse would otherwise be skipped
            # silently, leaving the sqlite backend's flattened type in place.
            if _COLUMN_START.match(line):
                msg = f"could not parse column definition in {table}: {line!r}"
                raise MdbtoolsError(msg)
            continue

        access_type = column["type"].strip()
        if access_type not in _ACCESS_TYPES:
            msg = (
                f"unmapped Access type {access_type!r} on "
                f"{table}.{column['column']}; add it to _ACCESS_TYPES"
            )
            raise MdbtoolsError(msg)

        spec = tuple(
            int(part) for part in (column["spec"] or "").split(",") if part.strip()
        )
        types[table, column["column"]] = _ACCESS_TYPES[access_type](spec)

    return types


def _structure(access_location: Path, workspace: Path) -> MetaData:
    """Reflect tables, columns and primary keys from mdbtools' SQLite DDL.

    The DDL is executed into a throwaway SQLite file and reflected, so
    SQLAlchemy parses it rather than a regex here. Its column *types* are
    unreliable and are overlaid afterwards by :func:`_native_types`.

    Args:
        access_location: Path to the ``.accdb`` or ``.mdb`` file.
        workspace: Directory to hold the throwaway SQLite file.

    Returns:
        Reflected metadata, with types still to be corrected.

    """
    ddl = _run("mdb-schema", "--not-null", str(access_location), "sqlite")

    scratch = workspace / "structure.sqlite"
    engine = create_engine(f"sqlite:///{scratch}")
    with engine.begin() as connection:
        # executescript, not a statement-per-semicolon split: the DDL carries
        # `--` comment lines, and splitting on ";" silently drops whatever
        # follows them.
        connection.connection.executescript(ddl)

    schema = MetaData()
    schema.reflect(bind=engine)
    engine.dispose()

    # Access ships internal tables that are not part of the data model: MSys*
    # catalogues, and ~TMPCLP* clipboard scratch tables left behind by whoever
    # built the file. DPM 4.3 carries two of the latter.
    for name in list(schema.tables):
        if name.startswith(_SYSTEM_TABLE_PREFIXES):
            schema.remove(schema.tables[name])

    return schema


def _apply_native_types(
    schema: MetaData,
    types: Mapping[tuple[str, str], TypeEngine[Any]],
) -> None:
    """Replace reflected column types with the native Access ones.

    Args:
        schema: Metadata from :func:`_structure`, modified in place.
        types: Mapping from :func:`_native_types`.

    """
    for table in schema.tables.values():
        for column in table.columns:
            if native := types.get((table.name, column.name)):
                column.type = native


def _apply_declared_foreign_keys(
    access_location: Path,
    schema: MetaData,
) -> int:
    """Add foreign keys declared in ``MSysRelationships``.

    Includes relationships Access marks "do not enforce", which
    ``mdb-schema --relations`` omits.

    Args:
        access_location: Path to the ``.accdb`` or ``.mdb`` file.
        schema: Metadata to add foreign keys to, modified in place.

    Returns:
        Number of foreign keys added.

    """
    exported = _run(
        "mdb-export",
        "--quote=",
        str(access_location),
        "MSysRelationships",
    )

    added = 0
    for row in csv.DictReader(StringIO(exported, newline="")):
        source = schema.tables.get(row["szObject"])
        target = schema.tables.get(row["szReferencedObject"])
        if source is None or target is None:
            # MSys* tables are not part of the reflected schema.
            continue

        column = source.columns.get(row["szColumn"])
        referenced = target.columns.get(row["szReferencedColumn"])
        if column is None or referenced is None or column.foreign_keys:
            continue

        column.append_foreign_key(ForeignKey(referenced))
        added += 1

    return added


def _repair_guid(value: str) -> str | None:
    """Normalise an mdbtools GUID to the canonical 8-4-4-4-12 form.

    mdbtools renders replication IDs as ``{8-4-4-16}`` — the fourth hyphen is
    missing, because no CLI tool calls ``mdb_set_repid_fmt``. All 32 hex digits
    are present and correctly ordered, so the braces and hyphens are stripped
    and the canonical grouping reapplied.

    Uppercase and hyphenated is what the pyodbc path produces, so published
    databases already hold GUIDs in that form and this keeps them byte-equal.

    Args:
        value: Raw exported value.

    Returns:
        The GUID as ``XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX``, or None if the
        value did not hold 32 hex digits.

    """
    digits = _NON_HEX.sub("", value.lower())
    if len(digits) != _GUID_DIGITS:
        return None

    groups = (digits[:8], digits[8:12], digits[12:16], digits[16:20], digits[20:])
    return "-".join(groups).upper()


def _coerce(  # noqa: PLR0911
    column: Column[Any],
    value: str,
    poisoned: list[tuple[str, str]],
) -> object:
    """Convert one exported text value to a Python value.

    Args:
        column: Column the value belongs to, carrying its native type and
            Access's declared nullability.
        value: Raw exported text.
        poisoned: Accumulator of ``(table, column)`` for repaired dates.

    Returns:
        The coerced value.

    """
    column_type = column.type

    if value == NULL:
        # mdbtools exports Access's zero-length strings as the null sentinel,
        # losing the distinction. Access declaring the column NOT NULL settles
        # it: the value cannot have been NULL, so it was the empty string.
        # ItemCategory.Signature in DPM 4.3 has 8 such rows.
        if isinstance(column_type, String) and not column.nullable:
            return ""
        return None

    if isinstance(column_type, Boolean):
        return value not in ("0", "FALSE", "False")

    if isinstance(column_type, DateTime):
        if value.startswith(POISON_DATE):
            poisoned.append((column.table.name, column.name))
            return FAR_FUTURE
        return datetime.strptime(value, DATETIME_FORMAT)  # noqa: DTZ007

    if isinstance(column_type, Integer):
        return int(value) if value else None

    if isinstance(column_type, Float):
        return float(value) if value else None

    if isinstance(column_type, CHAR):
        return _repair_guid(value)

    return value


def _check_poisoned(poisoned: list[tuple[str, str]]) -> None:
    """Fail if a date was repaired in a column not known to hold sentinels.

    Args:
        poisoned: Accumulated ``(table, column)`` pairs.

    Raises:
        MdbtoolsError: If any pair is outside :data:`POISON_ALLOWLIST`.

    """
    unexpected = {pair for pair in poisoned if pair not in POISON_ALLOWLIST}
    if unexpected:
        columns = ", ".join(f"{table}.{column}" for table, column in sorted(unexpected))
        msg = (
            f"mdbtools produced {POISON_DATE} in unexpected columns: {columns}. "
            f"The true value is unrecoverable from mdbtools, so it cannot be "
            f"repaired safely. Verify these values with a non-mdbtools reader, "
            f"then add them to POISON_ALLOWLIST if they are open-ended dates."
        )
        raise MdbtoolsError(msg)

    for table, column in sorted(set(poisoned)):
        repaired = poisoned.count((table, column))
        logger.info(
            "Repaired %d out-of-range date(s) in %s.%s to %s",
            repaired,
            table,
            column,
            FAR_FUTURE.date(),
        )


def _rows(
    access_location: Path,
    table: Table,
    poisoned: list[tuple[str, str]],
) -> Rows:
    """Export one table and return its rows as coerced dictionaries.

    Args:
        access_location: Path to the ``.accdb`` or ``.mdb`` file.
        table: Reflected table, carrying native column types.
        poisoned: Accumulator of ``(table, column)`` for repaired dates.

    Returns:
        One dictionary per row, keyed by column name.

    """
    exported = _run(
        "mdb-export",
        f"--null={NULL}",
        f"--datetime-format={DATETIME_FORMAT}",
        f"--date-format={DATETIME_FORMAT}",
        str(access_location),
        table.name,
    )

    return [
        {
            name: _coerce(table.columns[name], value, poisoned)
            for name, value in row.items()
            if name in table.columns
        }
        for row in csv.DictReader(StringIO(exported, newline=""))
    ]


def read_schema_and_rows(access_location: Path) -> tuple[MetaData, dict[str, Rows]]:
    """Read an Access database's schema and rows using mdbtools.

    The returned metadata carries tables, columns with native types, primary
    keys and the relationships Access declares. Naming-convention foreign keys
    and data-derived nullability are applied afterwards, by the same
    source-agnostic code the pyodbc path uses.

    Rows are read eagerly, matching the pyodbc path, which also materialises
    every row before creating the target schema.

    Args:
        access_location: Path to the ``.accdb`` or ``.mdb`` file.

    Returns:
        The metadata, and a mapping from table name to its rows.

    Raises:
        MdbtoolsError: If mdbtools is missing, too old, fails, or produced an
            unrepairable date.

    """
    require()

    with TemporaryDirectory(prefix="dpm-mdbtools-") as workspace:
        schema = _structure(access_location, Path(workspace))

    _apply_native_types(schema, _native_types(access_location))
    declared = _apply_declared_foreign_keys(access_location, schema)
    logger.info(
        "Read %d tables and %d declared foreign keys with mdbtools",
        len(schema.tables),
        declared,
    )

    poisoned: list[tuple[str, str]] = []
    rows = {
        name: _rows(access_location, table, poisoned)
        for name, table in schema.tables.items()
    }
    _check_poisoned(poisoned)

    return schema, rows
