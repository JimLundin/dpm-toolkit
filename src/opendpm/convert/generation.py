"""Model generator for generating SQLAlchemy models from database metadata."""

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Column, Enum, MetaData, Table

indent = "    "


def normalise_name(name: str) -> str:
    """Normalise column names by applying heuristics."""
    # Clean up column endings
    if name.endswith("GUID"):
        name = "Concept"
    if name.endswith("VID"):
        name = name.replace("VID", "Version")
    if name.startswith("DPM"):
        name = name.removeprefix("DPM")

    # Once all the "ID" endings are removed
    # We can apply other heuristics
    name = name.removesuffix("ID")
    if name.endswith("Code"):
        name = name.removesuffix("Code")
    if name.endswith("Oper"):
        name = name.replace("Oper", "Operation")
    if name == "Org":
        name = "Organisation"
    return name


class Model:
    """Custom generator for SQLAlchemy models."""

    def __init__(self, metadata: MetaData) -> None:
        """Initialize the model generator."""
        self.metadata = metadata
        self.imports: dict[str, set[str]] = defaultdict(set)
        self.base = "DPM"

    def render(self) -> str:
        """Generate SQLAlchemy models from database metadata."""
        self.imports["__future__"].add("annotations")

        models = [
            self._object(table)
            if table.primary_key or table.foreign_keys
            else self._table(table)
            for table in self.metadata.tables.values()
        ]

        return self._file(models)

    def _file(self, models: list[str]) -> str:
        """Render the complete model file."""
        base_class = self._base_class()
        imports = self._imports()
        header = [
            '"""SQLAlchemy models generated from DPM by the OpenDPM project."""',
            imports,
            base_class,
        ]

        return "\n".join(header + models)

    def _base_class(self) -> str:
        """Generate the base class definition."""
        self.imports["sqlalchemy.orm"].add("DeclarativeBase")
        return (
            f"class {self.base}(DeclarativeBase):\n"
            f'{indent}"""Base class for all DPM models."""'
        )

    def _imports(self) -> str:
        """Generate import statements."""
        return "\n".join(
            f"from {module} import {', '.join(names)}" if names else f"import {module}"
            for module, names in self.imports.items()
        )

    def _table(self, table: Table) -> str:
        """Generate a SQLAlchemy model for a table."""
        self.imports["sqlalchemy"].add("Table as SQLATable")

        lines = [f'"{table.name}"', f"{self.base}.metadata"]
        lines.extend(self._column(column) for column in table.columns)

        return f"{table.name} = SQLATable(\n{indent}{f',\n{indent}'.join(lines)}\n)\n"

    def _column(self, column: Column[Any]) -> str:
        """Generate a SQLAlchemy column."""
        sql_type = column.type.__class__.__name__
        self.imports["sqlalchemy"].add("Column")
        self.imports["sqlalchemy"].add(sql_type)
        return f'Column("{column.name}", {sql_type})'

    def _object(self, table: Table) -> str:
        """Generate a SQLAlchemy model for a table."""
        lines = [f"class {table.name}({self.base}):"]
        lines.append(f'{indent}"""Auto-generated model for the {table.name} table."""')
        lines.append("")
        lines.append(f'{indent}__tablename__ = "{table.name}"')
        lines.append("")
        lines.extend(self._mapped_column(column) for column in table.columns)
        lines.append("")
        lines.append(
            f"{indent}{self._mapper_args(table)}",
        ) if not table.primary_key else None
        lines.extend(self._table_relations(table))
        lines.append("")

        return "\n".join(lines)

    def _mapper_args(self, table: Table) -> str:
        """Generate a SQLAlchemy mapper for a table."""
        foreign_keys = (fk.column.name for fk in table.foreign_keys)
        self.imports["typing"].update(("ClassVar", "Any"))

        return (
            f"__mapper_args__: ClassVar[Any]"
            f' = {{"primary_key": ({", ".join(foreign_keys)})}}\n'
        )

    def _mapped_column(self, column: Column[Any]) -> str:
        """Generate SQLAlchemy column definition."""
        python_type = self._python_type(column)

        self.imports["sqlalchemy.orm"].add("Mapped")
        declaration = f"{indent}{column.name}: Mapped[{python_type}]"

        kwargs = self._column_key_attributes(column)
        fks = self._column_foreign_keys(column)

        if not fks and not kwargs:
            return declaration

        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        fks_str = ", ".join(fks)

        combined_args = ", ".join(filter(None, [fks_str, kwargs_str]))

        self.imports["sqlalchemy.orm"].add("mapped_column")
        return f"{declaration} = mapped_column({combined_args})"

    def _python_type(self, column: Column[Any]) -> str:
        """Get Python type for a column."""
        column_type = column.type
        python_type = column_type.python_type
        python_type_name = python_type.__name__

        if python_type == date:
            self.imports["datetime"].add("date")
        elif python_type == datetime:
            self.imports["datetime"].add("datetime")
        elif python_type == Decimal:
            self.imports["decimal"].add("Decimal")

        if isinstance(column_type, Enum):
            self.imports["typing"].add("Literal")
            enum_values = [f'"{enum}"' for enum in column_type.enums]  # type: ignore unknown
            python_type_name = f"Literal[{', '.join(enum_values)}]"

        return f"{python_type_name} | None" if column.nullable else python_type_name

    def _column_key_attributes(self, column: Column[Any]) -> dict[str, Any]:
        """Process primary key attributes of a column."""
        kwargs: dict[str, Any] = {}
        if column.primary_key:
            kwargs["primary_key"] = True

        return kwargs

    def _column_foreign_keys(self, column: Column[Any]) -> list[str]:
        """Process foreign keys of a column."""
        if not column.foreign_keys:
            return []

        self.imports["sqlalchemy"].add("ForeignKey")
        return [
            f'ForeignKey("{fk.column.table.name}.{fk.column.name}")'
            for fk in column.foreign_keys
        ]

    def _table_relations(self, table: Table) -> list[str]:
        """Generate SQLAlchemy relationship definitions."""
        return [
            self._relation(column, fk.column.table)
            for column in table.columns
            for fk in column.foreign_keys
        ]

    def _relation(self, src_col: Column[Any], ref_table: Table) -> str:
        """Generate a SQLAlchemy relationship definition."""
        src_name = normalise_name(src_col.name)
        if src_name == src_col.name:
            src_name = f"{src_name}_"

        src_type = f"{ref_table.name} | None" if src_col.nullable else ref_table.name

        self.imports["sqlalchemy.orm"].update(("Mapped", "relationship"))
        return (
            f"{indent}{src_name}: Mapped[{src_type}]"
            f" = relationship(foreign_keys=[{src_col.name}])"
        )
