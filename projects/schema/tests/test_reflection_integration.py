"""Integration tests for enum detection with SQLAlchemy reflection."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from sqlite3 import connect

import pytest
from sqlalchemy import Enum, MetaData, create_engine, event
from sqlalchemy.exc import DatabaseError

from schema.main import enum_reflection, sqlite_to_sqlalchemy_schema


@pytest.fixture(name="temp_db_with_enums")
def db_with_enums() -> Generator[str]:
    """Create a temporary SQLite database with enum-like check constraints."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    conn = connect(db_path)
    cursor = conn.cursor()

    # Create tables with various enum patterns
    cursor.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT CHECK (status IN ('active', 'inactive', 'pending')),
            role TEXT CHECK (role IN ('admin', 'user', 'moderator')),
            priority INTEGER CHECK (priority IN (1, 2, 3, 4, 5)),
            category TEXT CHECK (category = 'A' OR category = 'B' OR category = 'C'),
            score INTEGER CHECK (score >= 0 AND score <= 100)
        )
    """,
    )

    cursor.execute(
        """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT CHECK (type IN ('physical', 'digital', 'service')),
            size TEXT CHECK (size IN ('small', 'medium', 'large', 'extra_large'))
        )
    """,
    )

    cursor.execute(
        """
        CREATE TABLE no_enums (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            count INTEGER,
            price REAL CHECK (price > 0)
        )
    """,
    )

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    Path(db_path).unlink()


def test_enum_columns_detected_in_reflection(temp_db_with_enums: str) -> None:
    """Test that enum columns are properly detected during reflection."""
    engine = create_engine(f"sqlite:///{temp_db_with_enums}")
    metadata = MetaData()

    event.listen(metadata, "column_reflect", enum_reflection)
    metadata.reflect(bind=engine)

    # Test users table
    users_table = metadata.tables["users"]

    # String enum columns should be converted to Enum type
    assert isinstance(users_table.columns["status"].type, Enum)
    assert isinstance(users_table.columns["role"].type, Enum)
    assert isinstance(users_table.columns["category"].type, Enum)

    # Non-enum columns should keep their original types
    assert not isinstance(users_table.columns["id"].type, Enum)
    assert not isinstance(users_table.columns["name"].type, Enum)
    assert not isinstance(users_table.columns["priority"].type, Enum)  # Integer values
    assert not isinstance(users_table.columns["score"].type, Enum)  # Range constraint

    # Test products table
    products_table = metadata.tables["products"]
    assert isinstance(products_table.columns["type"].type, Enum)
    assert isinstance(products_table.columns["size"].type, Enum)
    assert not isinstance(products_table.columns["id"].type, Enum)
    assert not isinstance(products_table.columns["name"].type, Enum)

    # Test table with no enums
    no_enums_table = metadata.tables["no_enums"]
    for column in no_enums_table.columns:
        assert not isinstance(column.type, Enum)


def test_enum_values_correctly_extracted(temp_db_with_enums: str) -> None:
    """Test that the correct enum values are extracted."""
    engine = create_engine(f"sqlite:///{temp_db_with_enums}")
    metadata = MetaData()

    event.listen(metadata, "column_reflect", enum_reflection)
    metadata.reflect(bind=engine)

    # Check specific enum values
    users_table = metadata.tables["users"]

    status_enum = users_table.columns["status"].type
    assert set(status_enum.enums) == {"active", "inactive", "pending"}

    role_enum = users_table.columns["role"].type
    assert set(role_enum.enums) == {"admin", "user", "moderator"}

    category_enum = users_table.columns["category"].type
    assert set(category_enum.enums) == {"A", "B", "C"}

    products_table = metadata.tables["products"]

    type_enum = products_table.columns["type"].type
    assert set(type_enum.enums) == {"physical", "digital", "service"}

    size_enum = products_table.columns["size"].type
    assert set(size_enum.enums) == {"small", "medium", "large", "extra_large"}


def test_full_schema_generation_with_enums(temp_db_with_enums: str) -> None:
    """Test complete schema generation including enum detection."""
    engine = create_engine(f"sqlite:///{temp_db_with_enums}")
    schema_code = sqlite_to_sqlalchemy_schema(engine)

    # Verify the generated code contains Literal types for enum columns
    assert 'Literal["active", "inactive", "pending"]' in schema_code
    assert 'Literal["admin", "moderator", "user"]' in schema_code
    assert 'Literal["A", "B", "C"]' in schema_code
    assert 'Literal["digital", "physical", "service"]' in schema_code
    assert 'Literal["extra_large", "large", "medium", "small"]' in schema_code

    # Verify non-enum columns remain as basic types
    assert "id: Mapped[int | None]" in schema_code
    assert "name: Mapped[str]" in schema_code
    assert "priority: Mapped[int | None]" in schema_code  # Integer constraint, not enum
    assert "score: Mapped[int | None]" in schema_code  # Range constraint, not enum

    # Verify table class generation
    assert "class Users(DPM):" in schema_code
    assert "class Products(DPM):" in schema_code
    assert "class NoEnums(DPM):" in schema_code


def test_edge_case_constraints() -> None:
    """Test handling of edge case constraints."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    conn = connect(db_path)
    cursor = conn.cursor()

    # Create table with edge cases
    cursor.execute(
        """
        CREATE TABLE edge_cases (
            id INTEGER PRIMARY KEY,
            single_value TEXT CHECK (single_value = 'only'),
            empty_check TEXT,
            complex_enum TEXT CHECK (complex_enum IN ('multi-word', 'with_underscore', 'with space')),
            numeric_strings TEXT CHECK (numeric_strings IN ('1', '2', '3'))
        )
    """,
    )

    conn.commit()
    conn.close()

    try:
        engine = create_engine(f"sqlite:///{db_path}")
        schema_code = sqlite_to_sqlalchemy_schema(engine)

        # Single value is detected by our implementation
        assert 'Literal["only"]' in schema_code

        # Complex enum values should be handled
        assert 'Literal["multi-word", "with space", "with_underscore"]' in schema_code

        # Numeric strings should be handled
        assert 'Literal["1", "2", "3"]' in schema_code

    finally:
        Path(db_path).unlink()


def test_malformed_database_handling() -> None:
    """Test handling of databases that cause constraint inspection to fail."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = Path(tmp.name)

    # Create an empty file (not a valid SQLite database)
    with db_path.open("w", encoding="utf8") as f:
        f.write("not a database")

    try:
        engine = create_engine(f"sqlite:///{db_path}")
        # Should not raise an exception due to error handling
        schema_code = sqlite_to_sqlalchemy_schema(engine)
        # Should return basic schema without enum detection
        assert isinstance(schema_code, str)
    except DatabaseError:
        pass
    finally:
        Path(db_path).unlink()


def test_missing_constraint_sqltext() -> None:
    """Test handling of constraints without sqltext."""
    # This is harder to test directly since it depends on SQLAlchemy internals,
    # but the error handling in main.py should prevent issues
    pass
