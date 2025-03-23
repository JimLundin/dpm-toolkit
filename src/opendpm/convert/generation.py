"""Model generator for generating SQLAlchemy models from database metadata."""

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Column,
    Enum,
    MetaData,
    Table,
)

indent = "    "

type Imports = dict[str, set[str]]


class ModelGenerator:
    """Custom generator for SQLAlchemy models."""

    def __init__(self, metadata: MetaData) -> None:
        """Initialize the model generator."""
        self.metadata = metadata
        self.imports: Imports = defaultdict(set)

    def generate(self) -> str:
        """Generate SQLAlchemy models from database metadata."""
        self.imports["__future__"].add("annotations")

        # Generate model code
        models: list[str] = []
        for _, table in sorted(self.metadata.tables.items()):
            models.append(self.generate_model(table))

        # Combine everything into a single file
        return self._render_file(models)

    def generate_model(self, table: Table) -> str:
        """Generate a SQLAlchemy model for a table."""
        # Generate class definition
        lines = [f"class {table.name}(Base):"]

        # Add __tablename__ attribute
        lines.append(f'{indent}__tablename__ = "{table.name}"')

        # Add columns
        lines.extend([self._generate_column(column) for column in table.columns])

        # Add relationships
        lines.extend(self._generate_relationships(table))

        return "\n".join(lines)

    def _generate_imports(self) -> str:
        """Generate import statements."""
        return "\n".join(
            f"from {module} import {', '.join(sorted(names))}"
            if names
            else f"import {module}"
            for module, names in self.imports.items()
        )

    def _generate_base_class(self) -> str:
        """Generate the base class definition."""
        self.imports["sqlalchemy.orm"].add("DeclarativeBase")
        return f'class Base(DeclarativeBase):\n{indent}"""Base class for all models."""'

    def _generate_column[T](self, column: Column[T]) -> str:
        """Generate SQLAlchemy column definition."""
        # Determine Python type for the column
        python_type = self._get_python_type(column)

        # Process column attributes
        kwargs = self._process_column_key_attributes(column)
        args = self._process_column_foreign_keys(column)

        # Format kwargs
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        args_str = ", ".join(args)

        # Combine args and kwargs
        combined_args = ", ".join(filter(None, [args_str, kwargs_str]))

        self.imports["sqlalchemy.orm"].add("Mapped")
        declaration = f"{indent}{column.name}: Mapped[{python_type}]"

        # Generate the column definition
        if combined_args:
            self.imports["sqlalchemy.orm"].add("mapped_column")
            return f"{declaration} = mapped_column({combined_args})"

        return declaration

    def _process_column_key_attributes(self, column: Column[Any]) -> dict[str, Any]:
        """Process primary key attributes of a column."""
        kwargs: dict[str, Any] = {}
        if column.primary_key:
            kwargs["primary_key"] = True

        return kwargs

    def _process_column_foreign_keys(self, column: Column[Any]) -> list[str]:
        """Process foreign keys of a column."""
        foreign_keys: list[str] = []
        for fk in column.foreign_keys:
            ref_table = fk.column.table.name
            ref_column = fk.column.name
            self.imports["sqlalchemy"].add("ForeignKey")
            foreign_keys.append(f'ForeignKey("{ref_table}.{ref_column}")')

        return foreign_keys

    def _generate_relationships(self, table: Table) -> list[str]:
        """Generate SQLAlchemy relationship definitions."""
        relationships: list[str] = []

        # Find foreign keys that reference this table
        for ref_table_name, ref_table in self.metadata.tables.items():
            for column in ref_table.columns:
                for fk in column.foreign_keys:
                    if fk.column.table == table:
                        rel_def = self._create_relationship_definition(
                            table,
                            ref_table_name,
                            column,
                        )
                        relationships.append(rel_def)

        return relationships

    def _create_relationship_definition(
        self,
        table: Table,
        ref_table_name: str,
        column: Column[Any],
    ) -> str:
        """Create a relationship definition."""
        ref_name = self._to_relationship_name(ref_table_name)
        backref_name = self._to_relationship_name(table.name)

        # Determine if it's a one-to-many or many-to-one relationship
        if column.primary_key:
            # One-to-one relationship
            rel_type = f"{ref_table_name} | None"
        else:
            # One-to-many relationship
            rel_type = f"list[{ref_table_name}]"

        self.imports["sqlalchemy.orm"].update(("Mapped", "relationship"))
        return (
            f"{indent}{ref_name}: Mapped[{rel_type}]"
            f'= relationship("{ref_table_name}", back_populates="{backref_name}")'
        )

    def _get_python_type(self, column: Column[Any]) -> str:
        """Get Python type for a column."""
        col_type = column.type

        python_type = col_type.python_type
        python_type_name = python_type.__name__

        if python_type == date:
            self.imports["datetime"].add("date")
        elif python_type == datetime:
            self.imports["datetime"].add("datetime")
        elif python_type == Decimal:
            self.imports["decimal"].add("Decimal")
        elif isinstance(col_type, Enum):
            # Handle Enum types
            self.imports["typing"].add("Literal")
            enums = [f'"{enum}"' for enum in col_type.enums]  # type: ignore unknown
            python_type_name = f"Literal[{', '.join(enums)}]"

        # Handle nullable columns
        if column.nullable:
            python_type_name = f"{python_type_name} | None"

        return python_type_name

    def _to_relationship_name(self, table_name: str) -> str:
        """Convert table name to relationship name."""
        if table_name.endswith("VID"):
            table_name = table_name.removesuffix("VID")
            table_name += "Version"

        if table_name.endswith("ID"):
            table_name = table_name.removesuffix("ID")

        return table_name

    def _render_file(self, models: list[str]) -> str:
        """Render the complete model file."""
        # Generate file header
        base_class = self._generate_base_class()
        imports = self._generate_imports()
        header = ['"""SQLAlchemy models generated from DPM."""', imports, base_class]

        # Combine header and models
        return "\n".join(header + models)
