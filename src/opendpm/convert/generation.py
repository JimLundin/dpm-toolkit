"""Model generator for generating SQLAlchemy models from database metadata."""

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from inflect import engine
from sqlalchemy import Column, Enum, MetaData, Table

indent = "    "

inflect = engine()


def pluralize(string: str) -> str:
    """Pluralize a string."""
    words = string.split("[A-Z]")
    plural = inflect.plural(words[-1])  # type: ignore pyright incompat
    return "".join(words[:-1] + [plural])


def normalise_column_name(name: str) -> str:
    """Normalise column name by removing VID and ID."""
    if name.endswith("GUID"):
        return "Category"
    return name.replace("VID", "Version").replace("ID", "")


class Model:
    """Custom generator for SQLAlchemy models."""

    def __init__(self, metadata: MetaData) -> None:
        """Initialize the model generator."""
        self.metadata = metadata
        self.imports: dict[str, set[str]] = defaultdict(set)

    def render(self) -> str:
        """Generate SQLAlchemy models from database metadata."""
        self.imports["__future__"].add("annotations")

        models = [self._table(table) for table in self.metadata.tables.values()]

        return self._file(models)

    def _table(self, table: Table) -> str:
        """Generate a SQLAlchemy model for a table."""
        lines = [f"class {table.name}(Base):"]
        lines.append('"""Auto-generated model."""')
        lines.append(f'{indent}__tablename__ = "{table.name}"')
        lines.append("")
        lines.extend(self._mapped_column(column) for column in table.columns)
        lines.append("")
        lines.extend(self._table_relations(table))

        return "\n".join(lines)

    def _imports(self) -> str:
        """Generate import statements."""
        return "\n".join(
            f"from {module} import {', '.join(names)}" if names else f"import {module}"
            for module, names in self.imports.items()
        )

    def _base_class(self) -> str:
        """Generate the base class definition."""
        self.imports["sqlalchemy.orm"].add("DeclarativeBase")
        return f'class Base(DeclarativeBase):\n{indent}"""Base class for all models."""'

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
        relationships: list[str] = []

        for column in table.columns:
            relationships.extend(
                self._rev_relation(table.name, fk.column.table.name, column)
                for fk in column.foreign_keys
            )

        # Find foreign keys that reference this table
        for ref_table in self.metadata.tables.values():
            for ref_column in ref_table.columns:
                relationships.extend(
                    self._relation(table.name, ref_table.name, ref_column)
                    for fk in ref_column.foreign_keys
                    if fk.column.table == table
                )

        return relationships

    def _rev_relation(self, table: str, ref_table: str, column: Column[Any]) -> str:
        """Create a reverse relationship definition."""
        rev_rel = normalise_column_name(column.name)
        rel = table
        if table != rev_rel:
            rel += rev_rel

        self.imports["sqlalchemy.orm"].update(("Mapped", "relationship"))
        return (
            f"{indent}{rev_rel}: Mapped[{ref_table}]"
            f' = relationship(back_populates="{rel}")'
        )

    def _relation(self, table: str, ref_table: str, column: Column[Any]) -> str:
        """Create a relationship definition."""
        rev_rel = normalise_column_name(column.name)
        rel = ref_table
        if table != rev_rel:
            rel += rev_rel

        if not column.primary_key:
            rel = pluralize(rel)

        rel_type = ref_table if column.primary_key else f"list[{ref_table}]"

        self.imports["sqlalchemy.orm"].update(("Mapped", "relationship"))
        return (
            f"{indent}{rel}: Mapped[{rel_type}]"
            f'= relationship(back_populates="{rev_rel}")'
        )

    def _relationship(
        self,
        src_col: Column[Any],
        ref_col: Column[Any],
        src_table: Table,
        ref_table: Table,
    ) -> tuple[str, str, str, str]:
        """Create a relationship definition."""
        src_col_name = normalise_column_name(src_col.name)
        ref_col_name = normalise_column_name(ref_col.name)

        if src_col_name == ref_table.name:
            src_name = src_col_name
        else:
            src_name = f"{src_table.name}{ref_table.name}"

        if ref_col_name == src_table.name:
            ref_name = ref_col_name
        else:
            ref_name = f"{ref_table.name}{src_table.name}"

        if src_col.primary_key and not ref_col.primary_key:
            src_name = pluralize(src_name)
            src_type = f"list[{ref_table.name}]"
        else:
            src_type = (
                f"{ref_table.name} | None" if src_col.nullable else ref_table.name
            )

        if ref_col.primary_key and not src_col.primary_key:
            ref_name = pluralize(ref_name)
            ref_type = f"list[{src_table.name}]"
        else:
            ref_type = (
                f"{src_table.name} | None" if ref_col.nullable else src_table.name
            )

        return src_name, src_type, ref_name, ref_type

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
