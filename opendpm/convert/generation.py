"""Model generator for generating SQLAlchemy models from database metadata."""

import warnings
from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Column, Enum, MetaData, Table
from sqlalchemy.exc import SAWarning

indent = "    "


def normalise_column_name(name: str) -> str:
    """Normalise column names by applying heuristics."""
    return name.removesuffix("GUID").replace("VID", "Version").removesuffix("ID")


def format_foreign_key(key: str) -> str:
    """Render a foreign key."""
    return f"ForeignKey({key})"


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

        # Due to circular FKs we get an SAWarning when doing a topo sort of the tables
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=SAWarning)
            sorted_tables = self.metadata.sorted_tables

        models = [
            self._generate_class(table)
            if table.primary_key or "RowGUID" in table.columns
            else self._generate_table(table)
            for table in sorted_tables
        ]

        return self._generate_file(models)

    def _generate_file(self, models: list[str]) -> str:
        """Render the complete model file."""
        base_class = self._generate_base_class()
        imports = self._generate_imports()
        header = [
            '"""SQLAlchemy models generated from DPM by the OpenDPM project."""',
            imports,
            base_class,
        ]

        return "\n".join(header + models)

    def _generate_base_class(self) -> str:
        """Generate the base class definition."""
        self.imports["sqlalchemy.orm"].add("DeclarativeMeta")
        return (
            "# We use DeclarativeMeta instead of DeclarativeBase\n"
            "# to be compatible with mypy and __mapper_args__\n"
            f"class {self.base}(metaclass=DeclarativeMeta):\n"
            f'{indent}"""Base class for all DPM models."""'
        )

    def _generate_imports(self) -> str:
        """Generate import statements."""
        return "\n".join(
            f"from {module} import {', '.join(names)}" if names else f"import {module}"
            for module, names in self.imports.items()
        )

    def _generate_table(self, table: Table) -> str:
        """Generate a SQLAlchemy model for a table."""
        lines = (
            f'"{table.name}"',
            f"{self.base}.metadata",
            *(self._generate_column(column) for column in table.columns),
        )

        self.imports["sqlalchemy"].add("Table as AlchemyTable")
        return (
            f"{table.name} = AlchemyTable(\n{indent}{f',\n{indent}'.join(lines)}\n)\n"
        )

    def _generate_column(self, column: Column[Any]) -> str:
        """Generate a SQLAlchemy column."""
        sql_type = column.type.__class__.__name__
        self.imports["sqlalchemy"].add(sql_type)
        if isinstance(column.type, Enum):
            enum_values = (f'"{value}"' for value in column.type.enums)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            sql_type = f"Enum({', '.join(sorted(enum_values))})"
        self.imports["sqlalchemy"].add("Column")
        return (
            f'Column("{column.name}", {sql_type})'
            if column.nullable
            else f'Column("{column.name}", {sql_type}, nullable={column.nullable})'
        )

    def _generate_class(self, table: Table) -> str:
        """Generate a SQLAlchemy model for a table."""
        noqa = "" if table.name.isalpha() else "# noqa: N801"
        lines = (
            f"class {table.name}({self.base}):{noqa}",
            f'{indent}"""Auto-generated model for the {table.name} table."""',
            f'{indent}__tablename__ = "{table.name}"\n',
            f"{indent}# We quote the references to avoid circular dependencies"
            if table.name == "Concept"
            else "",
            *(self._generate_mapped_column(column) for column in table.columns),
            f"\n{indent}{self._generate_mapper_args(table)}"
            if not table.primary_key
            else "",
            *self._generate_relationships(table),
        )

        return "\n".join(lines)

    def _generate_mapper_args(self, table: Table) -> str:
        """Generate a SQLAlchemy mapper for a table."""
        if table.columns.get("RowGUID") is None:
            return ""

        self.imports["typing"].add("ClassVar")
        return '__mapper_args__: ClassVar = {"primary_key": (RowGUID,)}\n'

    def _generate_mapped_column(self, column: Column[Any]) -> str:
        """Generate SQLAlchemy column definition."""
        name = column.name
        python_type = self._get_python_type(column)

        self.imports["sqlalchemy.orm"].add("Mapped")
        declaration = f"{indent}{name}: Mapped[{python_type}]"

        kwargs = self._generate_column_key_attributes(column)
        fks = self._generate_column_foreign_keys(column)

        # if the column name does not start with an uppercase letter, add a noqa
        noqa = "# noqa: N815" if not name[0].isupper() and len(name) > 1 else ""

        if not kwargs and not fks:
            return f"{declaration}{noqa}"

        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        fks_str = ", ".join(fks)

        combined_args = ", ".join(filter(None, [fks_str, kwargs_str]))

        self.imports["sqlalchemy.orm"].add("mapped_column")
        return f"{declaration} = mapped_column({combined_args}){noqa}"

    def _get_python_type(self, column: Column[Any]) -> str:
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
            enum_values = [f'"{enum}"' for enum in column_type.enums]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            python_type_name = f"Literal[{', '.join(sorted(enum_values))}]"

        return f"{python_type_name} | None" if column.nullable else python_type_name

    def _generate_column_key_attributes(self, column: Column[Any]) -> dict[str, Any]:
        """Process primary key attributes of a column."""
        kwargs: dict[str, Any] = {}
        if column.primary_key:
            kwargs["primary_key"] = True

        return kwargs

    def _generate_column_foreign_keys(self, column: Column[Any]) -> Iterable[str]:
        """Process foreign keys of a column."""
        if not column.foreign_keys:
            return ()

        self.imports["sqlalchemy"].add("ForeignKey")

        foreign_keys: list[str] = []
        if column.table.name == "Concept":
            # For Concept, we quote the references to avoid circular dependencies
            foreign_keys.extend(
                f'"{fk.column.table.name}.{fk.column.name}"'
                for fk in column.foreign_keys
            )
        else:
            # Self referential FKs
            foreign_keys.extend(
                f'"{fk.column.name}"'
                if fk.column.name == column.name
                else fk.column.name
                for fk in column.foreign_keys
                if fk.column.table == column.table
            )
            # External pointing FKs
            foreign_keys.extend(
                f"{fk.column.table.name}.{fk.column.name}"
                for fk in column.foreign_keys
                if fk.column.table != column.table
            )

        return (format_foreign_key(fk) for fk in foreign_keys)

    def _generate_relationships(self, table: Table) -> list[str]:
        """Generate SQLAlchemy relationship definitions."""
        relationships: list[str] = []

        # This is split to avoid circular dependencies/race conditions
        relationships.extend(
            self._generate_relationship(column, fk.column)
            for column in table.columns
            for fk in column.foreign_keys
            if normalise_column_name(column.name) != fk.column.table.name
        )
        relationships.extend(
            self._generate_relationship(column, fk.column)
            for column in table.columns
            for fk in column.foreign_keys
            if normalise_column_name(column.name) == fk.column.table.name
        )
        return relationships

    def _generate_relationship(self, src_col: Column[Any], ref_col: Column[Any]) -> str:
        """Generate a SQLAlchemy relationship definition."""
        src_name = normalise_column_name(src_col.name)
        ref_table = ref_col.table
        if src_col.name == "RowGUID":  # for entities that reference Concept and RowGUID
            src_name = "RowConcept"
        if src_name == src_col.table.name:  # this covers PK to PK relationships
            src_name = ref_table.name
        if src_name == src_col.name:  # avoid name collision when the name = column name
            if ref_table.name in src_name:  # for 'CreatedRelease' and 'LanguageCode'
                src_name = ref_table.name
            else:  # for 'SubtypeDiscriminator'
                src_name = f"{src_name}{ref_table.name}"

        if src_col.table == ref_table and src_col.name == ref_col.name:
            src_name = "Self"

        src_type = f"{ref_table.name} | None" if src_col.nullable else ref_table.name

        self.imports["sqlalchemy.orm"].update(("Mapped", "relationship"))
        return (
            f"{indent}{src_name}: Mapped[{src_type}]"
            f" = relationship(foreign_keys={src_col.name})"
        )
