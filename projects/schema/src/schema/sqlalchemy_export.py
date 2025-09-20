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


def foreign_key(key: str) -> str:
    """Render a foreign key."""
    return f"ForeignKey({key})"


class JSONModel:
    """SQLAlchemy model generator from JSON schema."""

    def __init__(self, schema: DatabaseSchema) -> None:
        """Initialize the model generator."""
        self.schema = schema
        self.imports: dict[str, set[str]] = defaultdict(set)
        self.typing_imports: dict[str, set[str]] = defaultdict(set)
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
        typing_imports = self._generate_typing_imports()
        imports = self._generate_imports()
        header = [
            '"""SQLAlchemy models generated from DPM by the DPM Toolkit project."""',
            imports,
            typing_imports,
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

    def _generate_typing_imports(self) -> str:
        """Generate typing import statements."""
        self.imports["typing"].add("TYPE_CHECKING")
        return "if TYPE_CHECKING:\n" + "\n".join(
            f"{INDENT}from {module} import {', '.join(names)}"
            for module, names in self.typing_imports.items()
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
            f'Column("{column["name"]}", {sql_type_str})'
            if column["nullable"]
            else f'Column("{column["name"]}", {sql_type_str}, nullable={column["nullable"]})'
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

        self.typing_imports["typing"].add("ClassVar")
        return (
            "__mapper_args__: ClassVar = {\n"
            f'{INDENT}"primary_key": (row_guid,)\n'
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
        """Get Python type for a column."""
        sql_type = data_type_to_sql(column["type"])
        type_info = sql_to_python(sql_type)

        # Add imports for special types
        if type_info.module == "datetime":
            self.typing_imports["datetime"].add(type_info.name)
        elif type_info.module == "decimal":
            self.typing_imports["decimal"].add(type_info.name)
        elif type_info.module == "uuid":
            self.typing_imports["uuid"].add(type_info.name)
        elif type_info.module == "typing":
            self.imports["typing"].add(type_info.name)

        python_type_name = type_info.expression
        return f"{python_type_name} | None" if column["nullable"] else python_type_name

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
        """Process foreign keys of a column with domain-specific logic."""
        # Find foreign keys that reference this column
        foreign_keys: list[str] = []

        for fk in table_foreign_keys:
            for mapping in fk["column_mappings"]:
                if mapping["source_column"] == column["name"]:
                    target_table = fk["target_table"]
                    target_col = mapping["target_column"]

                    # Domain-specific foreign key reference logic
                    if table_name == "Concept":
                        # For Concept, we quote the references to avoid circular dependencies
                        foreign_keys.append(
                            f'"{pascal_case(target_table)}.{snake_case(target_col)}"',
                        )
                    elif target_table == table_name and target_col == column["name"]:
                        # Self-referential FKs
                        foreign_keys.append(f'"{snake_case(target_col)}"')
                    else:
                        # External pointing FKs
                        foreign_keys.append(
                            f"{pascal_case(target_table)}.{snake_case(target_col)}",
                        )

        if not foreign_keys:
            return []

        self.imports["sqlalchemy"].add("ForeignKey")
        return [foreign_key(fk) for fk in foreign_keys]

    def _generate_relationships(self, table: TableSchema) -> list[str]:
        """Generate SQLAlchemy relationship definitions using domain-specific logic."""
        relationships: list[str] = []

        # Process all foreign keys in the table
        for foreign_key_schema in table["foreign_keys"]:
            for column_mapping in foreign_key_schema["column_mappings"]:
                source_col_name = column_mapping["source_column"]
                target_table = foreign_key_schema["target_table"]
                target_col_name = column_mapping["target_column"]

                # Find the source column to get nullable info
                source_col = next(
                    col for col in table["columns"] if col["name"] == source_col_name
                )

                relationships.append(
                    self._generate_relationship(
                        table["name"],
                        source_col,
                        target_table,
                        target_col_name,
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
        src_name = relation_name(src_col["name"])

        # Domain-specific relationship naming rules
        if (
            src_col["name"] == "RowGUID"
        ):  # for entities that reference Concept and RowGUID
            src_name = "UniqueConcept"
        if src_name == src_table_name:  # this covers PK to PK relationships
            src_name = ref_table_name
        if (
            src_name == src_col["name"]
        ):  # avoid name collision when the name = column name
            if ref_table_name in src_name:  # for 'CreatedRelease' and 'LanguageCode'
                src_name = ref_table_name
            else:  # for 'SubtypeDiscriminator'
                src_name = f"{src_name}{ref_table_name}"

        if src_table_name == ref_table_name and src_col["name"] == ref_col_name:
            src_name = "Self"

        src_type = f"{ref_table_name} | None" if src_col["nullable"] else ref_table_name

        src_name = snake_case(src_name)

        if src_name in keyword.kwlist:
            src_name = f"{src_name}_"

        self.imports["sqlalchemy.orm"].update(("Mapped", "relationship"))
        return (
            f"{INDENT}{src_name}: Mapped[{pascal_case(src_type)}]"
            f" = relationship(foreign_keys={snake_case(src_col['name'])})"
        )


def schema_to_sqlalchemy(schema: DatabaseSchema) -> str:
    """Generate SQLAlchemy models from unified JSON schema."""
    model = JSONModel(schema)
    return model.render()
