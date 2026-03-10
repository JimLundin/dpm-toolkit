"""Tests for ForeignKey string-reference quoting.

Quoted (string-based) ForeignKey references must use the original DB
column name (``table_name.ColumnName``) rather than the Python attribute
name (``TableName.column_name``).  Self-referential FKs must also use
the quoted form so SQLAlchemy can resolve them after the class is fully
defined.
"""

from __future__ import annotations

from collections import defaultdict

from schema.sqlalchemy_export import (
    generate_column_definition,
    render_foreign_key,
    schema_to_sqlalchemy,
)
from schema.types import (
    ColumnSchema,
    DatabaseSchema,
    IntegerType,
    TableSchema,
    TextType,
)


def _make_column(
    name: str,
    table_name: str,
    *,
    primary_key: bool = False,
    nullable: bool = False,
    foreign_keys: list[ColumnSchema] | None = None,
) -> ColumnSchema:
    """Helper to build a minimal ColumnSchema."""
    return ColumnSchema(
        name=name,
        table_name=table_name,
        type=IntegerType(type="integer"),
        nullable=nullable,
        primary_key=primary_key,
        foreign_keys=foreign_keys or [],
    )


# ---------------------------------------------------------------------------
# Unit tests – render_foreign_key
# ---------------------------------------------------------------------------


class TestRenderForeignKey:
    """Tests for the ``render_foreign_key`` helper."""

    def test_unquoted_uses_python_names(self) -> None:
        """Unquoted refs use PascalCase class + snake_case attribute."""
        target = _make_column("ParentID", "my_table")
        result = render_foreign_key(target, quoted=False)
        assert result == "ForeignKey(MyTable.parent_id)"

    def test_quoted_uses_db_names(self) -> None:
        """Quoted refs use the original DB table and column names."""
        target = _make_column("ParentID", "my_table")
        result = render_foreign_key(target, quoted=True)
        assert result == 'ForeignKey("my_table.ParentID")'

    def test_quoted_preserves_table_name(self) -> None:
        """The table name in a quoted ref is __tablename__, not PascalCase."""
        target = _make_column("ConceptID", "Concept")
        result = render_foreign_key(target, quoted=True)
        # Must be the raw DB table name, not pascal_case("Concept")
        assert '"Concept.ConceptID"' in result

    def test_quoted_preserves_column_name(self) -> None:
        """The column name in a quoted ref is the DB name, not snake_case."""
        target = _make_column("RowGUID", "Concept")
        result = render_foreign_key(target, quoted=True)
        assert '"Concept.RowGUID"' in result

    def test_self_ref_column(self) -> None:
        """A self-referential FK should be expressed as a quoted string."""
        # In the old code, self_ref produced just '"ColumnName"'.
        # Now self-refs go through the quoted path, producing
        # '"TableName.ColumnName"'.
        target = _make_column("ParentID", "Category")
        # The caller sets quoted=True for self-refs
        result = render_foreign_key(target, quoted=True)
        assert result == 'ForeignKey("Category.ParentID")'


# ---------------------------------------------------------------------------
# Unit tests – generate_column_definition FK handling
# ---------------------------------------------------------------------------


class TestGenerateColumnDefinitionFK:
    """Test that generate_column_definition routes FKs correctly."""

    def test_self_ref_uses_quoted_string(self) -> None:
        """A column referencing its own table should use a quoted FK ref."""
        fk_target = _make_column("CategoryID", "Category", primary_key=True)
        column = _make_column(
            "ParentCategoryID",
            "Category",
            nullable=True,
            foreign_keys=[fk_target],
        )

        imports = defaultdict(set)
        defn = generate_column_definition(column, imports)

        # Must be a string ref (quoted) since it's self-referential
        assert 'ForeignKey("Category.CategoryID")' in defn

    def test_concept_table_uses_quoted_string(self) -> None:
        """FK columns on the Concept table should always be quoted."""
        fk_target = _make_column("ConceptID", "Concept", primary_key=True)
        column = _make_column(
            "ParentConceptID",
            "Concept",
            nullable=True,
            foreign_keys=[fk_target],
        )

        imports = defaultdict(set)
        defn = generate_column_definition(column, imports)

        assert 'ForeignKey("Concept.ConceptID")' in defn

    def test_normal_fk_uses_direct_ref(self) -> None:
        """A regular cross-table FK should use an unquoted Python ref."""
        fk_target = _make_column("ItemID", "Items", primary_key=True)
        column = _make_column(
            "ItemID",
            "Orders",
            foreign_keys=[fk_target],
        )

        imports = defaultdict(set)
        defn = generate_column_definition(column, imports)

        # Direct Python attribute reference, not a string
        assert "ForeignKey(Items.item_id)" in defn
        assert '"Items.' not in defn

    def test_concept_cross_table_fk_uses_quoted_string(self) -> None:
        """A FK on the Concept table pointing to another table is also quoted."""
        fk_target = _make_column("DomainID", "Domain", primary_key=True)
        column = _make_column(
            "DomainID",
            "Concept",
            foreign_keys=[fk_target],
        )

        imports = defaultdict(set)
        defn = generate_column_definition(column, imports)

        # Concept table FKs are always quoted
        assert 'ForeignKey("Domain.DomainID")' in defn


# ---------------------------------------------------------------------------
# Integration – schema_to_sqlalchemy with self-referential tables
# ---------------------------------------------------------------------------


class TestSchemaToSqlalchemySelfRef:
    """End-to-end tests with self-referential schemas."""

    def test_self_referential_table(self) -> None:
        """A table with a self-referential FK should use quoted string refs."""
        pk_col = _make_column("CategoryID", "Category", primary_key=True)
        parent_fk_target = _make_column(
            "CategoryID", "Category", primary_key=True,
        )
        parent_col = ColumnSchema(
            name="ParentCategoryID",
            table_name="Category",
            type=IntegerType(type="integer"),
            nullable=True,
            primary_key=False,
            foreign_keys=[parent_fk_target],
        )
        name_col = ColumnSchema(
            name="Name",
            table_name="Category",
            type=TextType(type="text", length=100),
            nullable=False,
            primary_key=False,
            foreign_keys=[],
        )

        table = TableSchema(
            name="Category",
            columns=[pk_col, parent_col, name_col],
            primary_keys=["CategoryID"],
            foreign_keys=[{"source": parent_col, "target": parent_fk_target}],
        )
        db = DatabaseSchema(name="test", tables=[table])

        code = schema_to_sqlalchemy(db)

        # The self-referential FK must be a quoted string with DB column name
        assert 'ForeignKey("Category.CategoryID")' in code
        # Should NOT contain an unquoted self-ref like ForeignKey(Category.category_i_d)
        assert "ForeignKey(Category." not in code

    def test_self_referential_generates_relationship(self) -> None:
        """A self-referential FK should produce a relationship definition."""
        pk_col = _make_column("DataTypeID", "DataType", primary_key=True)
        fk_target = _make_column("DataTypeID", "DataType", primary_key=True)
        parent_col = ColumnSchema(
            name="ParentDataTypeID",
            table_name="DataType",
            type=IntegerType(type="integer"),
            nullable=True,
            primary_key=False,
            foreign_keys=[fk_target],
        )

        table = TableSchema(
            name="DataType",
            columns=[pk_col, parent_col],
            primary_keys=["DataTypeID"],
            foreign_keys=[{"source": parent_col, "target": fk_target}],
        )
        db = DatabaseSchema(name="test", tables=[table])

        code = schema_to_sqlalchemy(db)

        assert 'ForeignKey("DataType.DataTypeID")' in code
        assert "parent_data_type: Mapped[DataType | None] = relationship(" in code
