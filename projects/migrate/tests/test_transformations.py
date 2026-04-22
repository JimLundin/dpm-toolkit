"""Tests for migrate.transformations."""

from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table

from migrate.transformations import heal_cross_table_foreign_keys


def _single_pk_table(metadata: MetaData, name: str, pk_column: str) -> Table:
    """Create a minimal table with one single-column primary key for tests."""
    return Table(
        name,
        metadata,
        Column(pk_column, Integer, primary_key=True),
        Column("Label", String),
    )


def test_heal_links_columns_to_unique_pk_targets() -> None:
    """Columns named like a unique PK in another table gain that FK."""
    metadata = MetaData()
    _single_pk_table(metadata, "Item", "ItemID")
    _single_pk_table(metadata, "Context", "ContextID")
    _single_pk_table(metadata, "Release", "StartReleaseID")
    compound = Table(
        "CompoundItemContext",
        metadata,
        Column("ItemID", Integer, primary_key=True),
        Column("StartReleaseID", Integer, primary_key=True),
        Column("ContextID", Integer),
    )

    heal_cross_table_foreign_keys(metadata)

    item_fk = next(iter(compound.columns["ItemID"].foreign_keys))
    ctx_fk = next(iter(compound.columns["ContextID"].foreign_keys))
    assert item_fk.target_fullname == "Item.ItemID"
    assert ctx_fk.target_fullname == "Context.ContextID"


def test_heal_skips_ambiguous_primary_key_names() -> None:
    """Shared PK names across tables leave referring columns untouched."""
    metadata = MetaData()
    _single_pk_table(metadata, "OperationVersion", "OperationVID")
    _single_pk_table(metadata, "OperationVersionData", "OperationVID")
    referrer = Table(
        "Referrer",
        metadata,
        Column("ID", Integer, primary_key=True),
        Column("OperationVID", Integer),
    )

    heal_cross_table_foreign_keys(metadata)

    assert not referrer.columns["OperationVID"].foreign_keys


def test_heal_does_not_self_reference() -> None:
    """A table's own PK column is never linked back to itself."""
    metadata = MetaData()
    item = Table(
        "Item",
        metadata,
        Column("ItemID", Integer, primary_key=True),
        Column("OtherID", Integer),
    )

    heal_cross_table_foreign_keys(metadata)

    assert not item.columns["ItemID"].foreign_keys


def test_heal_does_not_overwrite_declared_foreign_key() -> None:
    """Columns that already carry an FK are not given a duplicate one."""
    metadata = MetaData()
    _single_pk_table(metadata, "Item", "ItemID")
    other = Table(
        "Other",
        metadata,
        Column("ID", Integer, primary_key=True),
        Column("ItemID", Integer, ForeignKey("Item.ItemID")),
    )

    heal_cross_table_foreign_keys(metadata)

    assert len(list(other.columns["ItemID"].foreign_keys)) == 1
