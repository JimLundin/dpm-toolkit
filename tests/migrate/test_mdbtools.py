"""Tests for migrate.mdbtools.

The mdbtools binaries are not required: every test that would shell out uses
the ``recorded`` fixture to answer with captured output instead.
"""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import (
    CHAR,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
)
from sqlalchemy.types import TypeEngine

from dpm_toolkit.migrate import mdbtools
from dpm_toolkit.migrate.mdbtools import (
    FAR_FUTURE,
    NULL,
    MdbtoolsError,
    _check_poisoned,
    _coerce,
    _native_types,
    _repair_guid,
    _rows,
    _structure,
    version,
)

UNUSED = Path("unused.accdb")

ACCESS_DDL = """\
CREATE TABLE [Release]
 (
\t[ReleaseID]\t\t\tLong Integer NOT NULL,\x20
\t[Code]\t\t\tText (20) NOT NULL,\x20
\t[Date]\t\t\tDateTime NOT NULL,\x20
\t[Description]\t\t\tMemo/Hyperlink (255),\x20
\t[Ratio]\t\t\tDouble,\x20
\t[Timestamp]\t\t\tNumeric (19, 0) NOT NULL,\x20
\t[IsCurrent]\t\t\tBoolean NOT NULL,\x20
\t[RowGUID]\t\t\tReplication ID NOT NULL
);
"""

# mdb-schema puts `--` comment lines between statements. Splitting the DDL on
# ";" silently drops whatever follows them, so this fixture keeps one.
SQLITE_DDL = """\
CREATE TABLE `Release`
 (
\t`ReleaseID`\t\t\tINTEGER NOT NULL,\x20
\t`Code`\t\t\tvarchar (20) NOT NULL
\t, PRIMARY KEY (`ReleaseID`)
);

-- CREATE INDEXES ...
CREATE TABLE `Module`
 (
\t`ModuleID`\t\t\tINTEGER NOT NULL
\t, PRIMARY KEY (`ModuleID`)
);

CREATE TABLE `~TMPCLP531411`
 (
\t`Junk`\t\t\tINTEGER
);

CREATE TABLE `MSysNavPaneGroups`
 (
\t`Junk`\t\t\tINTEGER
);
"""

Recorder = Callable[[str], None]


@pytest.fixture
def recorded(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    """Return a function making every mdbtools call answer with given text."""

    def record(output: str) -> None:
        monkeypatch.setattr(mdbtools, "_run", lambda *_: output)

    return record


def _column(name: str, column_type: TypeEngine[Any]) -> Column[Any]:
    """Build a column attached to a table, since _coerce reads table.name."""
    column = Column(name, column_type)
    Table("Sample", MetaData(), column)
    return column


def test_native_types_maps_every_access_type(recorded: Recorder) -> None:
    """Each Access type becomes the SQLAlchemy type the pipeline expects."""
    recorded(ACCESS_DDL)

    types = _native_types(UNUSED)

    assert isinstance(types["Release", "ReleaseID"], Integer)
    assert isinstance(types["Release", "Date"], DateTime)
    assert isinstance(types["Release", "Ratio"], Float)
    assert isinstance(types["Release", "IsCurrent"], Boolean)


def test_native_types_preserves_text_length(recorded: Recorder) -> None:
    """Text lengths survive, which the sqlite backend would have dropped."""
    recorded(ACCESS_DDL)

    code = _native_types(UNUSED)["Release", "Code"]

    assert isinstance(code, String)
    assert code.length == 20


def test_native_types_maps_replication_id_to_char36(recorded: Recorder) -> None:
    """Replication IDs become CHAR(36), not the sqlite backend's INTEGER."""
    recorded(ACCESS_DDL)

    row_guid = _native_types(UNUSED)["Release", "RowGUID"]

    assert isinstance(row_guid, CHAR)
    assert row_guid.length == 36


def test_native_types_discards_memo_length(recorded: Recorder) -> None:
    """Memo columns become unbounded text, matching the pyodbc path.

    mdbtools reports Access's declared size for memo columns, but the ODBC
    path drops it. Honouring it diverges from published databases on 15
    columns, so it is dropped here too.
    """
    recorded(ACCESS_DDL)

    description = _native_types(UNUSED)["Release", "Description"]

    assert isinstance(description, Text)
    assert description.length is None


def test_native_types_rejects_unmapped_access_type(recorded: Recorder) -> None:
    """An unknown Access type fails loudly rather than being mistyped."""
    recorded(ACCESS_DDL.replace("Double", "Currency"))

    with pytest.raises(MdbtoolsError, match="unmapped Access type 'Currency'"):
        _native_types(UNUSED)


def test_structure_loads_ddl_past_comment_lines(
    recorded: Recorder,
    tmp_path: Path,
) -> None:
    """Statements after a `--` comment line are not dropped."""
    recorded(SQLITE_DDL)

    schema = _structure(UNUSED, tmp_path)

    assert set(schema.tables) == {"Release", "Module"}


def test_structure_recovers_primary_keys(
    recorded: Recorder,
    tmp_path: Path,
) -> None:
    """Primary keys come from the sqlite backend's DDL."""
    recorded(SQLITE_DDL)

    schema = _structure(UNUSED, tmp_path)

    primary_key = schema.tables["Release"].primary_key
    assert [column.name for column in primary_key.columns] == ["ReleaseID"]


def test_coerce_null_sentinel_becomes_none() -> None:
    """The --null sentinel is distinguishable from an empty string."""
    column = _column("Code", String(20))

    assert _coerce(column, NULL, []) is None
    assert _coerce(column, "", []) == ""


def test_coerce_boolean_reads_access_integers() -> None:
    """Access booleans export as 0/1."""
    column = _column("IsCurrent", Boolean())

    assert _coerce(column, "1", []) is True
    assert _coerce(column, "0", []) is False


def test_coerce_datetime_parses_exported_format() -> None:
    """Datetimes come back as datetimes, not strings."""
    column = _column("Date", DateTime())

    assert _coerce(column, "2024-02-06 00:00:00", []) == datetime(2024, 2, 6)  # noqa: DTZ001


def test_coerce_repairs_guid_missing_its_fourth_hyphen() -> None:
    """The 8-4-4-16 GUID mdbtools emits becomes the canonical 8-4-4-4-12 form.

    Published databases hold GUIDs uppercase and hyphenated, so this keeps the
    converted output byte-equal to them.
    """
    column = _column("RowGUID", CHAR(36))

    coerced = _coerce(column, "{083F7C98-D0CF-4D87-A2E19DC464712242}", [])

    assert coerced == "083F7C98-D0CF-4D87-A2E1-9DC464712242"


def test_coerce_text_passes_commas_through() -> None:
    """Memo columns are not mangled by the CSV reader."""
    column = _column("Description", Text())

    assert _coerce(column, "a, quoted value", []) == "a, quoted value"


def test_repair_guid_returns_none_when_not_32_digits() -> None:
    """A value that is not a whole GUID is a NULL GUID, not a partial one."""
    assert _repair_guid("{}") is None
    assert _repair_guid("{083F7C98-D0CF}") is None


def test_coerce_repairs_out_of_range_date_and_records_it() -> None:
    """The 1900-01-00 poison value becomes the open-ended sentinel."""
    column = _column("FromReferenceDate", DateTime())
    poisoned: list[tuple[str, str]] = []

    assert _coerce(column, "1900-01-00 00:00:00", poisoned) == FAR_FUTURE
    assert poisoned == [("Sample", "FromReferenceDate")]


def test_check_poisoned_accepts_allowlisted_columns() -> None:
    """Columns known to hold far-future dates are repaired silently."""
    _check_poisoned(
        [
            ("ModuleVersion", "FromReferenceDate"),
            ("OperationScope", "FromSubmissionDate"),
        ],
    )


def test_check_poisoned_rejects_unknown_column() -> None:
    """A poisoned date anywhere else fails the conversion."""
    with pytest.raises(MdbtoolsError, match=r"Table\.SomeDate"):
        _check_poisoned([("Table", "SomeDate")])


def test_version_parses_reported_string(recorded: Recorder) -> None:
    """The reported string is `mdbtools vX.Y.Z`."""
    recorded("mdbtools v1.0.1\n")

    assert version() == (1, 0, 1)


def test_version_rejects_unparseable_output(recorded: Recorder) -> None:
    """0.7.x rejects --version, which must not be read as a version."""
    recorded("mdb-export: invalid option -- '-'")

    with pytest.raises(MdbtoolsError, match="could not parse mdbtools version"):
        version()


def test_native_types_reads_numeric_precision_and_scale(recorded: Recorder) -> None:
    """`Numeric (19, 0)` carries two values, not one length."""
    recorded(ACCESS_DDL)

    timestamp = _native_types(UNUSED)["Release", "Timestamp"]

    assert isinstance(timestamp, Numeric)
    assert (timestamp.precision, timestamp.scale) == (19, 0)


def test_native_types_rejects_unparseable_column(recorded: Recorder) -> None:
    """A column line that does not parse fails instead of being skipped.

    Skipping it silently would leave the sqlite backend's flattened type in
    place, which is how an unmapped multi-argument type would slip through.
    """
    recorded(ACCESS_DDL.replace("Numeric (19, 0)", "Decimal <19 // 0>"))

    with pytest.raises(MdbtoolsError, match="could not parse column definition"):
        _native_types(UNUSED)


def test_structure_drops_access_internal_tables(
    recorded: Recorder,
    tmp_path: Path,
) -> None:
    """MSys catalogues and ~TMPCLP scratch tables are not part of the model."""
    recorded(SQLITE_DDL)

    schema = _structure(UNUSED, tmp_path)

    assert set(schema.tables) == {"Release", "Module"}


def test_coerce_restores_empty_string_on_not_null_column() -> None:
    """A NOT NULL text column cannot have been NULL, so it was ``''``.

    mdbtools reports Access's zero-length strings as NULL. Where Access
    declares the column NOT NULL the ambiguity resolves; ItemCategory.Signature
    in DPM 4.3 has 8 such rows.
    """
    column = Column("Signature", String(255), nullable=False)
    Table("ItemCategory", MetaData(), column)

    assert _coerce(column, NULL, []) == ""


def test_coerce_keeps_null_on_nullable_column() -> None:
    """On a nullable column the sentinel stays NULL; it cannot be resolved."""
    column = Column("Description", String(255), nullable=True)
    Table("Item", MetaData(), column)

    assert _coerce(column, NULL, []) is None


def test_rows_preserves_newlines_inside_quoted_fields(recorded: Recorder) -> None:
    """Memo text spanning lines keeps its line endings.

    Reading the export with ``splitlines()`` would split inside the quoted
    field and silently concatenate the parts, and decoding with universal
    newlines would rewrite CRLF to LF.
    """
    recorded('Label\r\n"first line\r\nsecond line"\r\n')
    column = Column("Label", String(255), nullable=True)
    table = Table("HeaderVersion", MetaData(), column)

    rows = _rows(UNUSED, table, [])

    assert rows == [{"Label": "first line\r\nsecond line"}]
