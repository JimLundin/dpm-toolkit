"""Schema transformation utilities for database conversion."""

from collections import defaultdict
from collections.abc import Iterator
from typing import Any

from schema.type_registry import enum_candidate
from sqlalchemy import Column, ForeignKey, MetaData, Row, Table

type Row_ = dict[str, Any]
type Rows = list[Row_]
type Columns = set[Column[Any]]
type EnumByColumn = dict[Column[Any], set[str]]


def parse_rows(
    table: Table,
    rows: Iterator[Row[Any]],
) -> tuple[Rows, EnumByColumn, Columns]:
    """Analyze rows for enum values and nullable columns.

    Values are passed through without casting — type recovery is handled
    downstream by the schema module using column naming conventions.
    """
    parsed_rows: Rows = []
    enum_by_column: EnumByColumn = defaultdict(set)
    nullable_columns: Columns = set()

    for row in rows:
        row_dict = row._asdict()  # pyright: ignore[reportPrivateUsage]

        for column_name, value in row_dict.items():
            table_column = table.columns[column_name]

            if value is None:
                nullable_columns.add(table_column)
                continue

            if enum_candidate(table_column) and isinstance(value, str):
                enum_by_column[table_column].add(value)

        parsed_rows.append(row_dict)

    return parsed_rows, enum_by_column, nullable_columns


def add_foreign_key_to_table(
    table: Table,
    source_column_name: str,
    target_relationship: str,
) -> None:
    """Set missing foreign keys."""
    column = table.columns.get(source_column_name)
    if column is None or column.foreign_keys:
        return
    column.append_foreign_key(ForeignKey(target_relationship))


def add_self_referential_foreign_keys(table: Table) -> None:
    """Detect and add self-referential foreign keys.

    If a column name ends with the table's own single-column primary key
    name, treat it as a self-referential FK (e.g. ``ParentNodeID`` →
    ``NodeID``, ``OwnerClassID`` → ``ClassID``).
    """
    pk_cols = list(table.primary_key.columns)
    if len(pk_cols) != 1:
        return
    pk_name = pk_cols[0].name
    for column in table.columns:
        if column.name != pk_name and column.name.endswith(pk_name):
            add_foreign_key_to_table(
                table,
                column.name,
                f"{table.name}.{pk_name}",
            )


def add_foreign_keys_to_table(table: Table) -> None:
    """Set missing foreign keys."""
    add_foreign_key_to_table(table, "RowGUID", "Concept.ConceptGUID")
    add_foreign_key_to_table(table, "ParentItemID", "Item.ItemID")
    add_self_referential_foreign_keys(table)

    # GroupOperID uses a truncated name (OperID ≠ OperationID), so the
    # ends-with-PK heuristic above cannot detect it automatically.
    add_foreign_key_to_table(table, "GroupOperID", "Operation.OperationID")


def _pick_canonical_owner(
    owners: list[tuple[Table, Column[Any]]],
) -> tuple[Table, Column[Any]] | None:
    """Choose one table as the canonical FK target for a PK name.

    Every candidate whose name prefixes *every* candidate's name is a
    valid owner: a single-owner list qualifies its lone entry (the
    name trivially prefixes itself); a group like ``OperationVersion``
    and ``OperationVersionData`` picks ``OperationVersion``. The
    accumulator returns early when a second qualifying candidate
    appears — that is the ambiguous case where no unique prefix wins.
    """
    names = [table.name for table, _ in owners]
    winner: tuple[Table, Column[Any]] | None = None
    for candidate in owners:
        if not all(name.startswith(candidate[0].name) for name in names):
            continue
        if winner is not None:
            return None
        winner = candidate
    return winner


def _resolve_pk_targets(
    schema: MetaData,
) -> dict[str, tuple[Table, Column[Any]]]:
    """Map each usable single-column PK name to its canonical (table, col)."""
    pk_owners: dict[str, list[tuple[Table, Column[Any]]]] = defaultdict(list)
    for table in schema.tables.values():
        match list(table.primary_key.columns):
            case [pk]:
                pk_owners[pk.name].append((table, pk))

    return {
        pk_name: canonical
        for pk_name, owners in pk_owners.items()
        if (canonical := _pick_canonical_owner(owners)) is not None
    }


def heal_cross_table_foreign_keys(schema: MetaData) -> None:
    """Link columns that share a name with another table's single-column PK.

    Some source releases ship tables whose FK constraints were dropped or
    never declared (e.g. 4.3-draft lost ``ChangeLogAttribute.ActionID`` →
    ``ChangeLog.ActionID``). The DPM schema uses globally unique PK names
    (``ItemID``, ``ContextID``, ``AttributeID``...), so whenever a PK name
    identifies exactly one table we can safely link same-named columns to
    it. When several tables share a PK name, ``_pick_canonical_owner``
    resolves the ambiguity for the prefix case (``OperationVID`` →
    ``OperationVersion``, not ``OperationVersionData``). The comprehension
    filter skips columns that already carry an FK and any self-reference
    from the canonical table's own PK column.
    """
    targets = _resolve_pk_targets(schema)
    fresh_fks = [
        (column, target[1])
        for table in schema.tables.values()
        for column in table.columns
        if not column.foreign_keys
        and (target := targets.get(column.name)) is not None
        and target[0] is not table
    ]
    for column, target_column in fresh_fks:
        column.append_foreign_key(ForeignKey(target_column))
