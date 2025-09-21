"""SQLAlchemy code generation from unified JSON schema."""

import keyword
from collections import defaultdict
from re import sub
from typing import Any

from schema.type_conversion import data_type_to_sql, sql_to_python, sql_to_string
from schema.types import ColumnSchema, DatabaseSchema, ForeignKeySchema, TableSchema

INDENT = "    "


def pascal_case(name: str) -> str:
    """Normalise table names to PascalCase."""
    return "".join(word[0].upper() + word[1:] for word in name.split("_"))


def relation_name(name: str) -> str:
    """Normalise relation names for DPM domain."""
    return name.removesuffix("GUID").replace("VID", "Version").removesuffix("ID")


def snake_case(name: str) -> str:
    """Convert a name to snake_case."""
    return sub("([a-z0-9])([A-Z])|([A-Z])([A-Z][a-z])", r"\1\3_\2\4", name).lower()


def foreign_key(key: str, *, quoted: bool = False) -> str:
    """Render a foreign key with optional quoting."""
    return f'ForeignKey("{key}")' if quoted else f"ForeignKey({key})"


class JSONModel:
    """SQLAlchemy model generator from JSON schema."""

    def __init__(self, schema: DatabaseSchema) -> None:
        """Initialize the model generator."""
        self.schema = schema
        self.imports: dict[str, set[str]] = defaultdict(set)
        self.base = "DPM"

    def render(self) -> str:
        """Generate SQLAlchemy models from JSON schema."""
        self.imports["__future__"].add("annotations")

        models = [
            (
                self._generate_class(table)
                if table["primary_keys"] or self._has_row_guid(table)
                else self._generate_table(table)
            )
            for table in self.schema["tables"]
        ]

        return self._generate_file(models)

    def _has_row_guid(self, table: TableSchema) -> bool:
        """Check if table has RowGUID column."""
        return any(col["name"] == "RowGUID" for col in table["columns"])

    def _generate_file(self, models: list[str]) -> str:
        """Render the complete model file."""
        base_class = self._generate_base_class()
        imports = self._generate_imports()
        header = [
            '"""SQLAlchemy models generated from DPM by the DPM Toolkit project."""',
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
            f'{INDENT}"""Base class for all DPM models."""'
        )

    def _generate_imports(self) -> str:
        """Generate import statements."""
        return "\n".join(
            f"from {module} import {', '.join(names)}" if names else f"import {module}"
            for module, names in self.imports.items()
        )

    def _generate_table(self, table: TableSchema) -> str:
        """Generate a SQLAlchemy Table for tables without primary keys."""
        lines = (
            f'"{table["name"]}"',
            f"{self.base}.metadata",
            *(self._generate_table_column(column) for column in table["columns"]),
        )

        self.imports["sqlalchemy"].add("Table as AlchemyTable")
        return f"""{pascal_case(table["name"])} = AlchemyTable(
            \n{INDENT}{f",\n{INDENT}".join(lines)}\n)\n"""

    def _generate_table_column(self, column: ColumnSchema) -> str:
        """Generate a SQLAlchemy column for Table definition."""
        sql_type = data_type_to_sql(column["type"])
        sql_type_str = sql_to_string(sql_type)

        # Add SQL type to imports
        sql_type_name = sql_type.__class__.__name__
        self.imports["sqlalchemy"].add(sql_type_name)

        self.imports["sqlalchemy"].add("Column")
        return (
            f'Column("{column["name"]}", {sql_type_str}, nullable={column["nullable"]})'
        )

    def _generate_class(self, table: TableSchema) -> str:
        """Generate a SQLAlchemy model class."""
        return "\n".join(
            (
                f"class {pascal_case(table['name'])}({self.base}):",
                f'{INDENT}"""Auto-generated model for the {table["name"]} table."""',
                f'{INDENT}__tablename__ = "{table["name"]}"\n',
                (
                    f"{INDENT}# We quote the references to avoid circular dependencies"
                    if table["name"] == "Concept"
                    else ""
                ),
                *(
                    self._generate_mapped_column(column, table)
                    for column in table["columns"]
                ),
                (
                    ""
                    if table["primary_keys"]
                    else f"\n{INDENT}{self._generate_mapper_args(table)}"
                ),
                *self._generate_relationships(table),
            ),
        )

    def _generate_mapper_args(self, table: TableSchema) -> str:
        """Generate a SQLAlchemy mapper for tables with RowGUID but no primary key."""
        row_guid_col = next(
            (col for col in table["columns"] if col["name"] == "RowGUID"),
            None,
        )
        if row_guid_col is None or row_guid_col["nullable"]:
            return ""

        self.imports["typing"].add("ClassVar")
        return (
            "__mapper_args__: ClassVar = {\n"
            f'{INDENT}"primary_key": ({snake_case(row_guid_col["name"])},)\n'
            "}\n"
        )

    def _generate_mapped_column(self, column: ColumnSchema, table: TableSchema) -> str:
        """Generate SQLAlchemy mapped column definition."""
        name = snake_case(column["name"])
        python_type = self._get_python_type(column)

        self.imports["sqlalchemy.orm"].add("Mapped")
        declaration = f"{INDENT}{name}: Mapped[{python_type}]"

        fks = self._generate_column_foreign_keys(
            column,
            table["foreign_keys"],
            table["name"],
        )
        kwargs = self._generate_column_key_attributes(column, table["primary_keys"])

        combined_args = ", ".join(
            (f'"{column["name"]}"', *fks, *(f"{k}={v}" for k, v in kwargs.items())),
        )

        self.imports["sqlalchemy.orm"].add("mapped_column")
        return f"{declaration} = mapped_column({combined_args})"

    def _get_python_type(self, column: ColumnSchema) -> str:
        """Get Python type for a column using structured type info."""
        sql_type = data_type_to_sql(column["type"])
        type_info = sql_to_python(sql_type)

        self.imports[type_info.module].add(type_info.name)

        return (
            f"{type_info.expression} | None"
            if column["nullable"]
            else type_info.expression
        )

    def _generate_column_key_attributes(
        self,
        column: ColumnSchema,
        primary_keys: list[str],
    ) -> dict[str, Any]:
        """Process primary key attributes of a column."""
        kwargs: dict[str, Any] = {}
        if column["name"] in primary_keys:
            kwargs["primary_key"] = True
        return kwargs

    def _generate_column_foreign_keys(
        self,
        column: ColumnSchema,
        table_foreign_keys: list[ForeignKeySchema],
        table_name: str,
    ) -> list[str]:
        """Generate foreign key references for a column."""
        foreign_keys: list[str] = []

        # Find all foreign key mappings that involve this column
        for fk_def in table_foreign_keys:
            for mapping in fk_def["column_mappings"]:
                if mapping["source_column"] == column["name"]:
                    target_table = fk_def["target_table"]
                    target_col = mapping["target_column"]

                    # Build the foreign key reference
                    if table_name == target_table:
                        # Self-referential foreign key
                        fk_ref = snake_case(target_col)
                        foreign_keys.append(foreign_key(fk_ref, quoted=True))
                    elif table_name == "Concept":
                        # Quote Concept table references to avoid circular dependencies
                        fk_ref = f"{pascal_case(target_table)}.{snake_case(target_col)}"
                        foreign_keys.append(foreign_key(fk_ref, quoted=True))
                    else:
                        # Standard external reference
                        fk_ref = f"{pascal_case(target_table)}.{snake_case(target_col)}"
                        foreign_keys.append(foreign_key(fk_ref))

        if foreign_keys:
            self.imports["sqlalchemy"].add("ForeignKey")

        return foreign_keys

    def _generate_relationships(self, table: TableSchema) -> list[str]:
        """Generate SQLAlchemy relationship definitions."""
        relationships: list[str] = []

        # Create column lookup for efficient access
        columns_by_name = {col["name"]: col for col in table["columns"]}

        # Generate one relationship per foreign key mapping
        for fk in table["foreign_keys"]:
            for mapping in fk["column_mappings"]:
                source_col = columns_by_name[mapping["source_column"]]

                relationships.append(
                    self._generate_relationship(
                        table["name"],
                        source_col,
                        fk["target_table"],
                        mapping["target_column"],
                    ),
                )

        return relationships

    def _generate_relationship(
        self,
        src_table_name: str,
        src_col: ColumnSchema,
        ref_table_name: str,
        ref_col_name: str,
    ) -> str:
        """Generate a SQLAlchemy relationship definition with domain-specific naming."""
        # Start with the domain-specific relation name
        rel_name = relation_name(src_col["name"])

        # Apply domain-specific naming rules
        if src_col["name"] == "RowGUID":
            rel_name = "UniqueConcept"
        elif rel_name == src_table_name:
            # PK to PK relationships use the target table name
            rel_name = ref_table_name
        elif rel_name == src_col["name"]:
            # Avoid name collision between relationship and column
            if ref_table_name in rel_name:
                rel_name = ref_table_name
            else:
                rel_name = f"{rel_name}{ref_table_name}"
        elif src_table_name == ref_table_name and src_col["name"] == ref_col_name:
            # Self-referential relationships
            rel_name = "Self"

        # Convert to snake_case and handle Python keywords
        rel_name = snake_case(rel_name)
        if rel_name in keyword.kwlist:
            rel_name = f"{rel_name}_"

        # Determine type annotation
        type_annotation = (
            f"{ref_table_name} | None" if src_col["nullable"] else ref_table_name
        )

        # Generate the relationship
        self.imports["sqlalchemy.orm"].update(("Mapped", "relationship"))
        return (
            f"{INDENT}{rel_name}: Mapped[{pascal_case(type_annotation)}]"
            f" = relationship(foreign_keys={snake_case(src_col['name'])})"
        )


def schema_to_sqlalchemy(schema: DatabaseSchema) -> str:
    """Generate SQLAlchemy models from unified JSON schema."""
    model = JSONModel(schema)
    return model.render()
