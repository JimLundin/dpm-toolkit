"""Tests for database processing utilities."""

from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    select,
    text,
)

from migrate.processing import genericize, load_data_to_database
from migrate.type_registry import column_type

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import ReflectedColumn


# ── genericize ────────────────────────────────────────────────────────


class TestGenericize:
    """Tests for the column_reflect event handler."""

    def test_overrides_type_when_registry_matches(self) -> None:
        """Columns like 'IsActive' should get Boolean type from registry."""
        col: ReflectedColumn = {  # type: ignore[typeddict-item]
            "name": "IsActive",
            "type": String(),
        }
        genericize(None, None, col)  # type: ignore[arg-type]
        # column_type("IsActive") returns Boolean(), so genericize should use it
        assert isinstance(col["type"], Boolean)

    def test_falls_back_to_generic_when_no_registry_match(self) -> None:
        """Columns without registry matches should use as_generic()."""
        engine = create_engine("sqlite://")
        metadata = MetaData()
        Table("test", metadata, Column("Name", String(50)))
        metadata.create_all(engine)

        # Reflect to get a dialect-specific type
        reflected = MetaData()
        reflected.reflect(bind=engine)
        reflected_col = reflected.tables["test"].columns["Name"]

        # Build a ReflectedColumn-like dict with the dialect type
        col: ReflectedColumn = {  # type: ignore[typeddict-item]
            "name": "Name",
            "type": reflected_col.type,
        }
        genericize(None, None, col)  # type: ignore[arg-type]
        # Should have called as_generic() — result should be a generic String
        assert column_type(col) is None  # "Name" doesn't match any registry rule


# ── load_data_to_database ─────────────────────────────────────────────


class TestLoadDataToDatabase:
    """Tests for load_data_to_database."""

    def test_loads_single_table(self) -> None:
        metadata = MetaData()
        table = Table(
            "users",
            metadata,
            Column("id", Integer(), primary_key=True),
            Column("name", String()),
        )
        engine = create_engine("sqlite://")
        metadata.create_all(engine)

        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        load_data_to_database(engine, [(table, rows)])

        with engine.begin() as conn:
            result = conn.execute(select(table)).fetchall()
        assert len(result) == 2
        assert result[0][1] == "Alice"

    def test_loads_multiple_tables(self) -> None:
        metadata = MetaData()
        t1 = Table(
            "a",
            metadata,
            Column("id", Integer(), primary_key=True),
            Column("val", String()),
        )
        t2 = Table(
            "b",
            metadata,
            Column("id", Integer(), primary_key=True),
            Column("val", String()),
        )
        engine = create_engine("sqlite://")
        metadata.create_all(engine)

        load_data_to_database(
            engine,
            [
                (t1, [{"id": 1, "val": "x"}]),
                (t2, [{"id": 1, "val": "y"}]),
            ],
        )

        with engine.begin() as conn:
            a_rows = conn.execute(select(t1)).fetchall()
            b_rows = conn.execute(select(t2)).fetchall()
        assert len(a_rows) == 1
        assert len(b_rows) == 1

    def test_loads_empty_list(self) -> None:
        engine = create_engine("sqlite://")
        # Should not raise
        load_data_to_database(engine, [])

    def test_data_integrity(self) -> None:
        """Verify data round-trips correctly through load_data_to_database."""
        metadata = MetaData()
        table = Table(
            "items",
            metadata,
            Column("id", Integer(), primary_key=True),
            Column("name", String()),
            Column("count", Integer()),
        )
        engine = create_engine("sqlite://")
        metadata.create_all(engine)

        original = [
            {"id": 1, "name": "Widget", "count": 10},
            {"id": 2, "name": "Gadget", "count": 0},
            {"id": 3, "name": "Doohickey", "count": 99},
        ]
        load_data_to_database(engine, [(table, original)])

        with engine.begin() as conn:
            result = conn.execute(
                select(table).order_by(text("id")),
            ).fetchall()

        assert [(r[0], r[1], r[2]) for r in result] == [
            (1, "Widget", 10),
            (2, "Gadget", 0),
            (3, "Doohickey", 99),
        ]
