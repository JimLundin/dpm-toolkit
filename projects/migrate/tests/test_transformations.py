"""Tests for schema transformation utilities."""

from collections.abc import Iterator
from datetime import date
from uuid import UUID

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Uuid,
    create_engine,
    insert,
    select,
)

from migrate.transformations import (
    add_foreign_key_to_table,
    add_foreign_keys_to_table,
    parse_rows,
)

# ── Helpers ───────────────────────────────────────────────────────────


def _make_table(*columns: Column[object], name: str = "test") -> Table:
    """Create a table bound to a fresh in-memory MetaData."""
    metadata = MetaData()
    return Table(name, metadata, *columns)


def _rows_from_dicts(
    table: Table,
    rows: list[dict[str, object]],
) -> Iterator[object]:
    """Insert rows into an in-memory SQLite table.

    Returns an iterator of Row objects.
    """
    engine = create_engine("sqlite://")
    table.metadata.create_all(engine)
    with engine.begin() as conn:
        if rows:
            conn.execute(insert(table), rows)
    with engine.begin() as conn:
        yield from conn.execute(select(table))


# ── parse_rows ────────────────────────────────────────────────────────


class TestParseRows:
    """Tests for parse_rows: casting, enum collection, and nullable detection."""

    def test_boolean_casting(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("IsActive", Boolean()),
        )
        rows = _rows_from_dicts(table, [{"id": 1, "IsActive": True}])
        casted, _, _ = parse_rows(table, rows)  # type: ignore[arg-type]
        assert casted[0]["IsActive"] is True

    def test_date_casting(self) -> None:
        # Use String for insertion (simulates Access providing raw strings)
        insert_table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("CreationDate", String()),
            name="test_date",
        )
        # Use Date type for parse_rows (the reflected schema after genericize)
        typed_table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("CreationDate", Date()),
            name="test_date",
        )
        rows = _rows_from_dicts(
            insert_table,
            [{"id": 1, "CreationDate": "2026-03-15"}],
        )
        casted, _, _ = parse_rows(typed_table, rows)  # type: ignore[arg-type]
        assert casted[0]["CreationDate"] == date(2026, 3, 15)

    def test_uuid_casting(self) -> None:
        # Use String for insertion (simulates Access providing raw strings)
        insert_table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("RowGUID", String()),
            name="test_uuid",
        )
        # Use Uuid type for parse_rows (the reflected schema after genericize)
        typed_table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("RowGUID", Uuid()),
            name="test_uuid",
        )
        uuid_str = "12345678-1234-5678-1234-567812345678"
        rows = _rows_from_dicts(insert_table, [{"id": 1, "RowGUID": uuid_str}])
        casted, _, _ = parse_rows(typed_table, rows)  # type: ignore[arg-type]
        assert casted[0]["RowGUID"] == UUID(uuid_str)

    def test_nullable_columns_tracked(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("Name", String()),
        )
        rows = _rows_from_dicts(
            table,
            [{"id": 1, "Name": None}, {"id": 2, "Name": "hello"}],
        )
        _, _, nullable = parse_rows(table, rows)  # type: ignore[arg-type]
        name_col = table.columns["Name"]
        assert name_col in nullable

    def test_non_null_column_not_in_nullable(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("Name", String()),
        )
        rows = _rows_from_dicts(
            table,
            [{"id": 1, "Name": "a"}, {"id": 2, "Name": "b"}],
        )
        _, _, nullable = parse_rows(table, rows)  # type: ignore[arg-type]
        name_col = table.columns["Name"]
        assert name_col not in nullable

    def test_enum_candidate_collection(self) -> None:
        """String columns ending with 'type' are enum candidates."""
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("ItemType", String()),
        )
        rows = _rows_from_dicts(
            table,
            [
                {"id": 1, "ItemType": "Alpha"},
                {"id": 2, "ItemType": "Beta"},
                {"id": 3, "ItemType": "Alpha"},
            ],
        )
        _, enum_by_column, _ = parse_rows(table, rows)  # type: ignore[arg-type]
        type_col = table.columns["ItemType"]
        assert type_col in enum_by_column
        assert enum_by_column[type_col] == {"Alpha", "Beta"}

    def test_empty_rows(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("Name", String()),
        )
        rows = _rows_from_dicts(table, [])
        casted, enum_by_col, nullable = parse_rows(table, rows)  # type: ignore[arg-type]
        assert casted == []
        assert len(enum_by_col) == 0
        assert len(nullable) == 0

    def test_identity_passthrough_for_unregistered_types(self) -> None:
        """String columns that are NOT enum candidates pass through unchanged."""
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("Name", String()),
        )
        rows = _rows_from_dicts(table, [{"id": 1, "Name": "hello"}])
        casted, _, _ = parse_rows(table, rows)  # type: ignore[arg-type]
        assert casted[0]["Name"] == "hello"


# ── add_foreign_key_to_table ─────────────────────────────────────────


class TestAddForeignKeyToTable:
    """Tests for add_foreign_key_to_table."""

    def test_adds_fk_to_column(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("RefID", String()),
        )
        add_foreign_key_to_table(table, "RefID", "OtherTable.OtherID")
        col = table.columns["RefID"]
        assert len(col.foreign_keys) == 1

    def test_no_op_if_column_missing(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
        )
        # Should not raise
        add_foreign_key_to_table(table, "NonExistent", "Other.ID")

    def test_no_op_if_fk_already_exists(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("RefID", String(), ForeignKey("Existing.ID")),
        )
        add_foreign_key_to_table(table, "RefID", "Other.ID")
        col = table.columns["RefID"]
        # Should still only have the original FK
        assert len(col.foreign_keys) == 1
        fk_target = next(iter(col.foreign_keys))._colspec  # noqa: SLF001
        assert fk_target == "Existing.ID"


# ── add_foreign_keys_to_table ────────────────────────────────────────


class TestAddForeignKeysToTable:
    """Tests for add_foreign_keys_to_table (hardcoded RowGUID / ParentItemID)."""

    def test_adds_rowguid_fk(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("RowGUID", Uuid()),
        )
        add_foreign_keys_to_table(table)
        col = table.columns["RowGUID"]
        assert len(col.foreign_keys) == 1

    def test_adds_parent_item_id_fk(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("ParentItemID", Integer()),
        )
        add_foreign_keys_to_table(table)
        col = table.columns["ParentItemID"]
        assert len(col.foreign_keys) == 1

    def test_no_crash_when_columns_absent(self) -> None:
        table = _make_table(
            Column("id", Integer(), primary_key=True),
            Column("SomeOtherColumn", String()),
        )
        # Should not raise
        add_foreign_keys_to_table(table)
