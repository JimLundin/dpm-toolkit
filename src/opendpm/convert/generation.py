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


def difference(str1: str, str2: str) -> tuple[str, str]:
    """Find the difference between two strings."""
    length = min(len(str1), len(str2))
    for i in range(length, 0, -1):
        prefix1, suffix1, comp1 = str1[:i], str1[i:], str1[:-i]
        prefix2, suffix2, comp2 = str2[:i], str2[i:], str2[:-i]
        if str2.endswith(prefix1):
            return suffix1, comp2
        if str1.endswith(prefix2):
            return comp1, suffix2

    return str1, str2


REFLECT = {
    "child": "parent",
    "parent": "child",
    "abstract": "concrete",
    "concrete": "abstract",
    "owner": "owned",
    "owned": "owner",
    "group": "grouped",
    "grouped": "group",
    "translator": "translating",
    "translating": "translator",
    "created": "",
}


def reflect(string: str) -> str:
    """Reflect a string."""
    reflection = REFLECT.get(string.lower(), None)
    return reflection.title() if reflection else string


def plural(string: str) -> str:
    """Pluralize a string."""
    if string.endswith(("s", "x")):
        return string + "es"
    if string.endswith("ey"):
        return string + "s"
    if string.endswith("y"):
        return string[:-1] + "ies"
    return string + "s"


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
        relationships = [
            self._relation(column, fk.column, table, fk.column.table)
            for column in table.columns
            for fk in column.foreign_keys
        ]

        # Find foreign keys that reference this table
        relationships.extend(
            self._relation(fk.column, ref_column, table, ref_table)
            for ref_table in self.metadata.tables.values()
            for ref_column in ref_table.columns
            for fk in ref_column.foreign_keys
            if fk.column.table == table
        )

        return relationships

    def _relation(
        self,
        src_col: Column[Any],
        ref_col: Column[Any],
        src_table: Table,
        ref_table: Table,
    ) -> str:
        """Generate a SQLAlchemy relationship definition."""
        src_name, ref_name = self._relation_name(
            src_col.name,
            ref_col.name,
            src_table.name,
            ref_table.name,
        )

        if ref_col.primary_key:
            src_type = ref_table.name
        else:
            src_name = plural(src_name)
            src_type = f"list[{ref_table.name}]"

        ref_name = plural(ref_name) if not src_col.primary_key else ref_name

        src_type = f"{src_type} | None" if src_col.nullable else src_type

        self.imports["sqlalchemy.orm"].update(("Mapped", "relationship"))
        return (
            f"{indent}{src_name}: Mapped[{src_type}]"
            f' = relationship(back_populates="{ref_name}")'
        )

    def _relation_name(
        self,
        src_col: str,
        ref_col: str,
        src_table: str,
        ref_table: str,
    ) -> tuple[str, str]:
        src_name = normalise_name(src_col)
        ref_name = normalise_name(ref_col)

        if src_name == ref_name:
            src_name = ref_table
            ref_name = src_table
        elif src_name in ref_name:
            comp1, _ = difference(ref_name, src_name)
            src_name = f"{reflect(comp1)}{src_name}"
        elif ref_name in src_name:
            comp2, _ = difference(src_name, ref_name)
            ref_name = f"{reflect(comp2)}{ref_name}"
        else:
            src_name, ref_name = f"{src_name}{ref_name}", f"{ref_name}{src_name}"

        if src_table in src_name:
            src_name, _ = difference(src_name, src_table)
            comp1, _ = difference(src_name, ref_table)
            src_name = f"{comp1}{ref_table}"
        if ref_table in ref_name:
            ref_name, _ = difference(ref_name, ref_table)
            comp2, _ = difference(ref_name, src_table)
            ref_name = f"{comp2}{src_table}"

        return src_name, ref_name
