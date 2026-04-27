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
    """Choose one table as the canonical FK target for a shared PK name.

    When several tables share a single-column PK (e.g. ``OperationVID``
    appears on both ``OperationVersion`` and ``OperationVersionData``),
    the shorter table whose name prefixes the others is treated as the
    owner; the longer names are auxiliary extensions that point at it.
    If no single name prefixes every candidate, the PK stays ambiguous.
    """
    names = [table.name for table, _ in owners]
    prefixes = [
        (table, column)
        for table, column in owners
        if all(other.startswith(table.name) for other in names)
    ]
    if len(prefixes) == 1:
        return prefixes[0]
    return None


def _resolve_pk_targets(
    schema: MetaData,
) -> dict[str, tuple[Table, Column[Any]]]:
    """Map each usable single-column PK name to its canonical (table, col)."""
    pk_owners: dict[str, list[tuple[Table, Column[Any]]]] = defaultdict(list)
    for table in schema.tables.values():
        pk_cols = list(table.primary_key.columns)
        if len(pk_cols) == 1:
            pk_owners[pk_cols[0].name].append((table, pk_cols[0]))

    targets: dict[str, tuple[Table, Column[Any]]] = {}
    for pk_name, owners in pk_owners.items():
        canonical = owners[0] if len(owners) == 1 else _pick_canonical_owner(owners)
        if canonical is not None:
            targets[pk_name] = canonical
    return targets


def heal_cross_table_foreign_keys(schema: MetaData) -> None:
    """Link columns that share a name with another table's single-column PK.

    Some source releases ship tables whose FK constraints were dropped or
    never declared (e.g. 4.3-draft lost ``ChangeLogAttribute.ActionID`` →
    ``ChangeLog.ActionID``). The DPM schema uses globally unique PK names
    (``ItemID``, ``ContextID``, ``AttributeID``...), so whenever a PK name
    identifies exactly one table we can safely link same-named columns to
    it. When several tables share a PK name, ``_pick_canonical_owner``
    resolves the ambiguity for the prefix case (``OperationVID`` →
    ``OperationVersion``, not ``OperationVersionData``). The
    ``target_table is table`` guard below prevents the canonical table's
    own PK column from being linked back to itself.
    """
    targets = _resolve_pk_targets(schema)

    for table in schema.tables.values():
        for column in table.columns:
            if column.foreign_keys:
                continue
            target = targets.get(column.name)
            if target is None:
                continue
            target_table, target_column = target
            if target_table is table:
                continue
            column.append_foreign_key(ForeignKey(target_column))
