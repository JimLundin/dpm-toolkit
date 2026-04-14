"""Clean SQLAlchemy code generation working directly from schema definitions."""

import keyword
from collections import defaultdict
from re import sub

from sqlalchemy.types import Date, DateTime

from schema.type_conversion import data_type_to_sql, sql_to_python, sql_to_string
from schema.types import ColumnSchema, DatabaseSchema, ForeignKeySchema, TableSchema

type Imports = dict[str, set[str]]


# Name of the hand-written module in the ``dpm2`` package that supplies
# the custom SQLAlchemy types used by the generated models.
_DPM_TYPES_MODULE = "dpm2.types"


def _register_dpm_type(sql_type: object, imports: Imports) -> str | None:
    """If ``sql_type`` has a DPM-custom counterpart, record its import.

    Returns the generated-code name for the custom type (e.g. ``"DPMDate"``)
    or ``None`` if the type needs no customisation. ``DateTime`` is checked
    before ``Date`` because ``DateTime`` is not a ``Date`` subclass in
    SQLAlchemy, but an explicit-first-match style is safer.
    """
    if isinstance(sql_type, DateTime):
        imports[_DPM_TYPES_MODULE].add("DPMDateTime")
        return "DPMDateTime"
    if isinstance(sql_type, Date):
        imports[_DPM_TYPES_MODULE].add("DPMDate")
        return "DPMDate"
    return None


def pascal_case(name: str) -> str:
    """Convert name to PascalCase."""
    return "".join(word[0].upper() + word[1:] for word in name.split("_"))


def snake_case(name: str) -> str:
    """Convert name to snake_case."""
    return sub("([a-z0-9])([A-Z])|([A-Z])([A-Z][a-z])", r"\1\3_\2\4", name).lower()


def clean_name(name: str) -> str:
    """Clean domain-specific suffixes from names."""
    return name.removesuffix("GUID").replace("VID", "Version").removesuffix("ID")


def has_row_guid(table: TableSchema) -> bool:
    """Check if table has RowGUID column."""
    return any(col["name"] == "RowGUID" for col in table["columns"])


def relationship_name(source: ColumnSchema, target: ColumnSchema) -> str:
    """Generate relationship name using DPM domain-specific naming rules."""
    src_col = source["name"]
    src_table = source["table_name"]
    tgt_col = target["name"]
    tgt_table = target["table_name"]

    # Start with the domain-specific relation name
    rel_name = clean_name(src_col)

    # Apply domain-specific naming rules
    if src_col == "RowGUID":
        rel_name = "UniqueConcept"
    elif rel_name == src_table:
        # PK to PK relationships use the target table name
        rel_name = tgt_table
    elif rel_name == src_col:
        # Avoid name collision between relationship and column
        rel_name = tgt_table if tgt_table in rel_name else f"{rel_name}{tgt_table}"
    elif src_table == tgt_table and src_col == tgt_col:
        # Self-referential relationships
        rel_name = "Self"

    # Convert to snake_case and handle Python keywords
    rel_name = snake_case(rel_name)
    if rel_name in keyword.kwlist:
        rel_name = f"{rel_name}_"

    return rel_name


def render_foreign_key(
    target: ColumnSchema,
    *,
    quoted: bool = False,
) -> str:
    """Render ForeignKey reference from target column."""
    if quoted:
        # String refs use __tablename__ and the actual DB column name
        ref = f'"{target["table_name"]}.{target["name"]}"'
    else:
        # Direct Python refs use the class name and mapped attribute name
        ref = f"{pascal_case(target['table_name'])}.{snake_case(target['name'])}"

    return f"ForeignKey({ref})"


def generate_column_definition(column: ColumnSchema, imports: Imports) -> str:
    """Generate mapped_column definition for a column."""
    # Build Python type annotation
    sql_type = data_type_to_sql(column["type"])
    # Register DPM-custom types (if any) so the registry's type_annotation_map
    # can route Mapped[date] / Mapped[datetime] through our TypeDecorators.
    _register_dpm_type(sql_type, imports)
    type_info = sql_to_python(sql_type)

    if type_info.module:
        if type_info.name:
            imports[type_info.module].add(type_info.name)
        else:
            imports.setdefault(type_info.module, set())

    python_type = (
        f"{type_info.expression} | None" if column["nullable"] else type_info.expression
    )

    # Build column arguments
    args = [f'"{column["name"]}"']

    # Add foreign key if present
    for fk_column in column["foreign_keys"]:
        is_self_ref = fk_column["table_name"] == column["table_name"]
        needs_quoting = column["table_name"] == "Concept" or is_self_ref
        imports["sqlalchemy"].add("ForeignKey")
        fk = render_foreign_key(fk_column, quoted=needs_quoting)
        args.append(fk)

    # Add primary key if needed
    if column["primary_key"]:
        args.append("primary_key=True")

    # Generate the definition
    imports["sqlalchemy.orm"].update(("Mapped", "mapped_column"))

    column_name = snake_case(column["name"])
    args_str = ", ".join(args)

    return f"\t{column_name}: Mapped[{python_type}] = mapped_column({args_str})"


def generate_relationship_definition(fk: ForeignKeySchema, imports: Imports) -> str:
    """Generate relationship definition from foreign key schema."""
    source = fk["source"]
    target = fk["target"]

    # Generate relationship name
    rel_name = relationship_name(source, target)

    # Build type annotation
    target_class = pascal_case(target["table_name"])
    type_def = f"{target_class} | None" if source["nullable"] else target_class

    # Generate the relationship
    imports["sqlalchemy.orm"].update(("Mapped", "relationship"))
    col_name = snake_case(source["name"])

    return f"\t{rel_name}: Mapped[{type_def}] = relationship(foreign_keys={col_name})"


def generate_class_definition(
    table: TableSchema,
    base_class: str,
    imports: Imports,
) -> str:
    """Generate complete SQLAlchemy class definition for a table."""
    class_name = pascal_case(table["name"])

    lines = [
        f"class {class_name}({base_class}):",
        f'\t"""Auto-generated model for the {table["name"]} table."""',
        f'\t__tablename__ = "{table["name"]}"',
        "",
    ]

    # Add comment for Concept table
    if table["name"] == "Concept":
        lines.append("\t# We quote the references to avoid circular dependencies")

    # Generate columns
    lines.extend(
        generate_column_definition(column, imports) for column in table["columns"]
    )
    # Add mapper args for RowGUID tables without primary keys
    if not table["primary_keys"] and has_row_guid(table):
        lines.extend(
            (
                "",
                "\t__mapper_args__: ClassVar = {",
                '\t    "primary_key": (row_guid,)',
                "\t}",
            ),
        )
        imports["typing"].add("ClassVar")

    # Generate relationships
    lines.append("")
    lines.extend(
        generate_relationship_definition(fk, imports) for fk in table["foreign_keys"]
    )
    return "\n".join(lines)


def generate_table_definition(
    table: TableSchema,
    base_class: str,
    imports: Imports,
) -> str:
    """Generate SQLAlchemy Table definition for tables without primary keys."""
    table_name = pascal_case(table["name"])

    args = [
        f'"{table["name"]}"',
        f"{base_class}.metadata",
    ]

    # Add columns
    for column in table["columns"]:
        sql_type = data_type_to_sql(column["type"])
        imports["sqlalchemy"].add("Column")

        # Prefer DPM-custom types (e.g. DPMDate) when applicable so DD/MM/YYYY
        # values parse correctly at read time.
        dpm_name = _register_dpm_type(sql_type, imports)
        if dpm_name is not None:
            type_str = dpm_name
        else:
            imports["sqlalchemy"].add(sql_type.__class__.__name__)
            type_str = sql_to_string(sql_type)

        args.append(
            f'Column("{column["name"]}", {type_str}, nullable={column["nullable"]})',
        )

    imports["sqlalchemy"].add("Table as AlchemyTable")
    args_str = ",\n\t".join(args)

    return f"{table_name} = AlchemyTable(\n\t{args_str}\n)"


def generate_imports(imports: Imports) -> str:
    """Generate import statements from collected imports."""
    lines = [
        f"from {module} import {', '.join(sorted(names))}"
        if names
        else f"import {module}"
        for module, names in imports.items()
    ]
    return "\n".join(lines)


def generate_base_class(base_name: str, imports: Imports | None = None) -> str:
    """Generate the base class definition.

    We use DeclarativeMeta instead of DeclarativeBase so that subclasses can
    override __mapper_args__ as a ClassVar without a mypy 'misc' error.
    DeclarativeMeta requires an explicit registry and metadata on the base.

    When DPM-custom date types are in use, the registry is constructed with a
    ``type_annotation_map`` so that ``Mapped[date]`` / ``Mapped[datetime]``
    annotations on the generated ORM models resolve to our ``TypeDecorator``
    wrappers (which accept Access-style DD/MM/YYYY strings at read time).
    """
    dpm_types = imports.get(_DPM_TYPES_MODULE, set()) if imports else set()
    entries: list[str] = []
    if "DPMDate" in dpm_types:
        entries.append("datetime.date: DPMDate")
    if "DPMDateTime" in dpm_types:
        entries.append("datetime.datetime: DPMDateTime")

    if entries:
        if imports is not None:
            imports.setdefault("datetime", set())
        registry_args = "type_annotation_map={" + ", ".join(entries) + "}"
    else:
        registry_args = ""

    return f"""_registry = registry({registry_args})

class {base_name}(metaclass=DeclarativeMeta):
    \"\"\"Base class for all DPM models.\"\"\"
    __abstract__ = True
    registry = _registry
    metadata = _registry.metadata"""


def schema_to_sqlalchemy(schema: DatabaseSchema) -> str:
    """Generate SQLAlchemy models from schema - clean and direct approach."""
    imports: Imports = defaultdict(set)
    imports["__future__"].add("annotations")
    imports["sqlalchemy.orm"].update(("DeclarativeMeta", "registry"))

    base_class = "DPM"

    # Generate models (force evaluation to populate imports)
    models = [
        (
            generate_class_definition(table, base_class, imports)
            if table["primary_keys"] or has_row_guid(table)
            else generate_table_definition(table, base_class, imports)
        )
        for table in schema["tables"]
    ]

    # Build the base class first so any side-effect imports it adds
    # (e.g. ``datetime`` for the type_annotation_map) are captured before
    # rendering the final import block.
    base_class_code = generate_base_class(base_class, imports)

    # Assemble final file
    parts = (
        '"""SQLAlchemy models generated from DPM by the DPM Toolkit project."""',
        "# ruff: noqa: TC001, TC002, TC003",
        generate_imports(imports),
        base_class_code,
        *models,
    )

    return "\n".join(parts)
