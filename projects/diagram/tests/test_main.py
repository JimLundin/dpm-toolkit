"""Tests for the main diagram generation functionality."""

import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from sqlalchemy import text

from diagram import read_only_sqlite, sqlite_to_diagram


@pytest.fixture(name="sample_database")
def social_media_sample_database() -> Generator[Path]:
    """Create a sample SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
        db_path = Path(f.name)

    # Create sample database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(100)
        )
    """,
    )

    cursor.execute(
        """
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            title VARCHAR(255) NOT NULL,
            content TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """,
    )

    cursor.execute(
        """
        CREATE TABLE comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            FOREIGN KEY (post_id) REFERENCES posts(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """,
    )

    # Insert sample data
    cursor.execute(
        "INSERT INTO users (email, name) VALUES (?, ?)",
        ("test@example.com", "Test User"),
    )
    cursor.execute(
        "INSERT INTO posts (user_id, title, content) VALUES (?, ?, ?)",
        (1, "Test Post", "Content"),
    )
    cursor.execute(
        "INSERT INTO comments (post_id, user_id, content) VALUES (?, ?, ?)",
        (1, 1, "Comment"),
    )

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    db_path.unlink()


def test_read_only_sqlite(sample_database: Path) -> None:
    """Test creating a read-only SQLAlchemy engine."""
    engine = read_only_sqlite(sample_database)
    assert engine is not None

    # Test that we can query the database
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM users"))
        assert result.scalar() == 1


def test_sqlite_to_diagram_json(sample_database: Path) -> None:
    """Test generating diagram JSON from SQLite database."""
    engine = read_only_sqlite(sample_database)
    diagram = sqlite_to_diagram(engine)

    # Verify new simplified structure
    assert "name" in diagram
    assert "tables" in diagram
    # No more metadata, relationships, or layout sections

    # Verify tables
    tables = diagram["tables"]
    assert len(tables) == 3

    table_names = {table["name"] for table in tables}
    assert table_names == {"users", "posts", "comments"}

    # Find users table and verify new structure
    users_table = next(table for table in tables if table["name"] == "users")
    assert "columns" in users_table
    assert "primary_key" in users_table
    assert "foreign_keys" in users_table
    assert users_table["primary_keys"] == ["id"]

    # Verify columns have simplified fields
    for column in users_table["columns"]:
        assert "name" in column
        assert "type" in column
        assert "nullable" in column
        # No more primary_key, foreign_key, unique, or default_value flags

    # Find posts table to verify foreign keys
    posts_table = next(table for table in tables if table["name"] == "posts")
    assert len(posts_table["foreign_keys"]) >= 1

    # Verify foreign key structure
    user_fk = posts_table["foreign_keys"][0]  # Should be the FK to users
    assert "target_table" in user_fk
    assert "column_mappings" in user_fk
    assert user_fk["target_table"] == "users"

    # Verify column mapping structure
    mapping = user_fk["column_mappings"][0]
    assert "source_column" in mapping
    assert "target_column" in mapping
    assert mapping["source_column"] == "user_id"
    assert mapping["target_column"] == "id"


def test_diagram_json_schema_compliance(sample_database: Path) -> None:
    """Test that generated JSON complies with new TypedDict schema."""
    engine = read_only_sqlite(sample_database)
    diagram = sqlite_to_diagram(engine)
    # Test root schema compliance
    assert isinstance(diagram["name"], str)
    assert isinstance(diagram["tables"], list)

    # Test each table has required fields
    for table in diagram["tables"]:
        assert isinstance(table["name"], str)
        assert isinstance(table["columns"], list)
        assert isinstance(table["primary_keys"], list)
        assert isinstance(table["foreign_keys"], list)

        # Test columns schema
        for column in table["columns"]:
            assert isinstance(column["name"], str)
            assert isinstance(column["type"], str)
            assert isinstance(column["nullable"], bool)
            # No more primary_key, foreign_key flags

        # Test primary key is list of strings
        for pk_col in table["primary_keys"]:
            assert isinstance(pk_col, str)

        # Test foreign keys schema
        for fk in table["foreign_keys"]:
            assert isinstance(fk["target_table"], str)
            assert isinstance(fk["column_mappings"], list)

            # Test column mappings
            for mapping in fk["column_mappings"]:
                assert isinstance(mapping["source_column"], str)
                assert isinstance(mapping["target_column"], str)
