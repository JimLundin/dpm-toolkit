"""Schema transformation utilities for database conversion."""

from collections import defaultdict
from collections.abc import Callable, Iterator
from datetime import date, datetime
from typing import Any

from schema.type_registry import date_column, enum_candidate
from sqlalchemy import Column, Date, DateTime, ForeignKey, Row, Table

type Field = str | date | datetime
type Row_ = dict[str, Any]
type Rows = list[Row_]
type Columns = set[Column[Any]]
type EnumByColumn = dict[Column[Any], set[str]]


def cast_date(raw_value: Field) -> date:
    """Cast a string value to date. ISO fast path, European fallback."""
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value
    if isinstance(raw_value, str):
        try:
            return date.fromisoformat(raw_value)
        except ValueError:
            pass
        try:
            return datetime.strptime(raw_value, "%d/%m/%Y").date()  # noqa: DTZ007
        except ValueError:
            pass
        msg = f"Cannot convert '{raw_value}' to date"
        raise ValueError(msg)
    msg = f"Cannot convert {type(raw_value).__name__} to date: {raw_value}"
    raise TypeError(msg)


def cast_datetime(raw_value: Field) -> datetime:
    """Cast a string value to datetime. ISO fast path, European fallback."""
    if isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, date):
        return datetime.combine(raw_value, datetime.min.time())
    if isinstance(raw_value, str):
        try:
            return datetime.fromisoformat(raw_value)
        except ValueError:
            pass
        try:
            return datetime.strptime(raw_value, "%d/%m/%Y %H:%M:%S")  # noqa: DTZ007
        except ValueError:
            pass
        try:
            return datetime.strptime(raw_value, "%d/%m/%Y")  # noqa: DTZ007
        except ValueError:
            pass
        msg = f"Cannot convert '{raw_value}' to datetime"
        raise ValueError(msg)
    msg = f"Cannot convert {type(raw_value).__name__} to datetime: {raw_value}"
    raise TypeError(msg)


def _date_caster(column: Column[Any]) -> Callable[[Any], Any] | None:
    """Get date caster for a column, or None if not a date column."""
    match date_column(column):
        case t if t is DateTime:
            return cast_datetime
        case t if t is Date:
            return cast_date
        case _:
            return None


def parse_rows(
    table: Table,
    rows: Iterator[Row[Any]],
) -> tuple[Rows, EnumByColumn, Columns]:
    """Analyze rows for enum values and nullable columns.

    Date/datetime values are cast to ensure consistent ISO storage in SQLite.
    Other values are passed through — type recovery is handled downstream
    by the schema module using column naming conventions.
    """
    parsed_rows: Rows = []
    enum_by_column: EnumByColumn = defaultdict(set)
    nullable_columns: Columns = set()

    # Pre-compute date casters for performance
    casters: dict[str, Callable[[Any], Any]] = {}
    for column in table.columns:
        if caster := _date_caster(column):
            casters[column.name] = caster

    for row in rows:
        row_dict = row._asdict()  # pyright: ignore[reportPrivateUsage]

        for column_name, value in row_dict.items():
            table_column = table.columns[column_name]

            if value is None:
                nullable_columns.add(table_column)
                continue

            if enum_candidate(table_column) and isinstance(value, str):
                enum_by_column[table_column].add(value)
            elif column_name in casters:
                row_dict[column_name] = casters[column_name](value)

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


def add_foreign_keys_to_table(table: Table) -> None:
    """Set missing foreign keys."""
    add_foreign_key_to_table(table, "RowGUID", "Concept.ConceptGUID")
    add_foreign_key_to_table(table, "ParentItemID", "Item.ItemID")
