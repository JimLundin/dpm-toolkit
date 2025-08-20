"""Tests for the Inspector class."""

from collections.abc import Iterator
from sqlite3 import Connection, OperationalError, Row, connect

import pytest

from compare.inspector import Inspector


@pytest.fixture(name="simple_db")
def create_simple_db() -> Iterator[Connection]:
    """Create a simple test database with basic schema."""
    conn = connect(":memory:")

    # Create test tables
    conn.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER
        )
    """,
    )

    conn.execute(
        """
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            title TEXT NOT NULL,
            content TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """,
    )

    conn.execute(
        """
        CREATE TABLE tags (
            name TEXT PRIMARY KEY,
            description TEXT
        )
    """,
    )

    # Insert test data
    conn.execute(
        "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
        ("Alice", "alice@example.com", 30),
    )
    conn.execute(
        "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
        ("Bob", "bob@example.com", 25),
    )

    conn.execute(
        "INSERT INTO posts (user_id, title, content) VALUES (?, ?, ?)",
        (1, "First Post", "Hello world"),
    )
    conn.execute(
        "INSERT INTO posts (user_id, title, content) VALUES (?, ?, ?)",
        (2, "Second Post", "More content"),
    )

    conn.execute(
        "INSERT INTO tags (name, description) VALUES (?, ?)",
        ("python", "Python programming"),
    )

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture(name="empty_db")
def create_empty_db() -> Iterator[Connection]:
    """Create an empty test database."""
    conn = connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture(name="no_pk_db")
def create_no_pk_db() -> Iterator[Connection]:
    """Create a database with a table that has no primary key."""
    conn = connect(":memory:")

    conn.execute(
        """
        CREATE TABLE logs (
            timestamp TEXT,
            message TEXT,
            level TEXT
        )
    """,
    )

    conn.execute(
        "INSERT INTO logs VALUES (?, ?, ?)",
        ("2024-01-01", "Test message", "INFO"),
    )

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture(name="system_tables_db")
def create_system_tables_db() -> Iterator[Connection]:
    """Create a database that might have system-like table names."""
    conn = connect(":memory:")

    # Regular table
    conn.execute("CREATE TABLE user_data (id INTEGER PRIMARY KEY, value TEXT)")

    # Table with name that looks like it could be system table but isn't
    conn.execute("CREATE TABLE my_sqlite_logs (id INTEGER PRIMARY KEY, info TEXT)")

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture(name="rowguid_db")
def create_rowguid_db() -> Iterator[Connection]:
    """Database with RowGUID columns (critical for DPM databases)."""
    conn = connect(":memory:")

    # Table with RowGUID - primary identification method for DPM
    conn.execute(
        """
        CREATE TABLE entities (
            id INTEGER PRIMARY KEY,
            RowGUID TEXT UNIQUE,
            name TEXT,
            value INTEGER
        )
    """,
    )

    # Insert test data with RowGUIDs
    conn.execute(
        "INSERT INTO entities (RowGUID, name, value) VALUES (?, ?, ?)",
        ("guid-001", "Entity 1", 100),
    )
    conn.execute(
        "INSERT INTO entities (RowGUID, name, value) VALUES (?, ?, ?)",
        ("guid-002", "Entity 2", 200),
    )

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture(name="composite_pk_db")
def create_composite_pk_db() -> Iterator[Connection]:
    """Database with composite primary keys."""
    conn = connect(":memory:")

    # Table with composite primary key
    conn.execute(
        """
        CREATE TABLE mappings (
            source_id INTEGER,
            target_id INTEGER,
            mapping_type TEXT,
            created_date TEXT,
            PRIMARY KEY (source_id, target_id, mapping_type)
        )
    """,
    )

    # Insert test data
    conn.execute(
        "INSERT INTO mappings VALUES (?, ?, ?, ?)",
        (1, 10, "direct", "2024-01-01"),
    )
    conn.execute(
        "INSERT INTO mappings VALUES (?, ?, ?, ?)",
        (1, 20, "indirect", "2024-01-02"),
    )
    conn.execute(
        "INSERT INTO mappings VALUES (?, ?, ?, ?)",
        (2, 10, "direct", "2024-01-03"),
    )

    conn.commit()
    yield conn
    conn.close()


def test_init_sets_row_factory(simple_db: Connection) -> None:
    """Test that Inspector sets row_factory correctly."""
    inspector = Inspector(simple_db)
    assert inspector.conn.row_factory is Row


def test_tables_returns_user_tables(simple_db: Connection) -> None:
    """Test that tables() returns all user tables."""
    inspector = Inspector(simple_db)
    tables = list(inspector.tables())

    assert set(tables) == {"users", "posts", "tags"}


def test_tables_empty_database(empty_db: Connection) -> None:
    """Test tables() with empty database."""
    inspector = Inspector(empty_db)
    tables = list(inspector.tables())

    assert not tables


def test_tables_excludes_system_tables(system_tables_db: Connection) -> None:
    """Test that tables() includes user tables with sqlite-like names."""
    inspector = Inspector(system_tables_db)
    tables = list(inspector.tables())

    # Should include both tables (neither starts with sqlite_)
    assert set(tables) == {"user_data", "my_sqlite_logs"}


def test_tables_excludes_real_system_tables(simple_db: Connection) -> None:
    """Test that tables() excludes actual sqlite_ system tables."""
    inspector = Inspector(simple_db)
    tables = list(inspector.tables())

    # Should not include any system tables (sqlite_master, sqlite_sequence, etc.)
    for table in tables:
        assert not table.startswith("sqlite_")

    # Should only have our user tables
    assert set(tables) == {"users", "posts", "tags"}


def test_cols_returns_column_info(simple_db: Connection) -> None:
    """Test that cols() returns correct column metadata."""
    inspector = Inspector(simple_db)
    cols = list(inspector.cols("users"))

    # Should have 4 columns
    assert len(cols) == 4

    # Check column names
    col_names = [col["name"] for col in cols]
    assert col_names == ["id", "name", "email", "age"]

    # Check primary key
    pk_cols = [col for col in cols if col["pk"]]
    assert len(pk_cols) == 1
    assert pk_cols[0]["name"] == "id"

    # Check not null constraint
    notnull_cols = [col["name"] for col in cols if col["notnull"]]
    assert "name" in notnull_cols


def test_cols_nonexistent_table(simple_db: Connection) -> None:
    """Test cols() with nonexistent table."""
    inspector = Inspector(simple_db)
    cols = list(inspector.cols("nonexistent"))

    assert not cols


def test_pks_single_primary_key(simple_db: Connection) -> None:
    """Test pks() with single primary key."""
    inspector = Inspector(simple_db)
    pks = list(inspector.pks("users"))

    assert pks == ["id"]


def test_pks_text_primary_key(simple_db: Connection) -> None:
    """Test pks() with text primary key."""
    inspector = Inspector(simple_db)
    pks = list(inspector.pks("tags"))

    assert pks == ["name"]


def test_pks_no_primary_key(no_pk_db: Connection) -> None:
    """Test pks() with table that has no primary key."""
    inspector = Inspector(no_pk_db)
    pks = list(inspector.pks("logs"))

    assert not pks


def test_rows_returns_data(simple_db: Connection) -> None:
    """Test that rows() returns table data."""
    inspector = Inspector(simple_db)
    rows = list(inspector.rows("users"))

    assert len(rows) == 2

    # Check first row
    assert rows[0]["name"] == "Alice"
    assert rows[0]["email"] == "alice@example.com"
    assert rows[0]["age"] == 30

    # Check second row
    assert rows[1]["name"] == "Bob"
    assert rows[1]["email"] == "bob@example.com"
    assert rows[1]["age"] == 25


def test_rows_empty_table(simple_db: Connection) -> None:
    """Test rows() with empty table."""
    # Create empty table
    simple_db.execute("CREATE TABLE empty_table (id INTEGER PRIMARY KEY)")
    simple_db.commit()

    inspector = Inspector(simple_db)
    rows = list(inspector.rows("empty_table"))

    assert not rows


def test_rows_nonexistent_table(simple_db: Connection) -> None:
    """Test rows() with nonexistent table raises exception."""
    inspector = Inspector(simple_db)

    with pytest.raises(OperationalError):
        list(inspector.rows("nonexistent"))


def test_rows_with_sql_injection_attempt(simple_db: Connection) -> None:
    """Test that rows() handles table names safely."""
    inspector = Inspector(simple_db)

    # This should raise an error, not execute malicious SQL
    with pytest.raises(OperationalError):
        list(inspector.rows("users; DROP TABLE users; --"))


def test_multiple_operations_on_same_connection(simple_db: Connection) -> None:
    """Test that multiple operations work correctly on the same connection."""
    inspector = Inspector(simple_db)

    # Multiple calls should work
    tables1 = list(inspector.tables())
    tables2 = list(inspector.tables())
    assert tables1 == tables2

    # Mixed operations
    cols = list(inspector.cols("users"))
    rows = list(inspector.rows("users"))
    pks = list(inspector.pks("users"))

    assert len(cols) == 4
    assert len(rows) == 2
    assert pks == ["id"]


def test_row_factory_provides_dict_access(simple_db: Connection) -> None:
    """Test that Row objects provide dict-like access."""
    inspector = Inspector(simple_db)
    rows = list(inspector.rows("users"))

    # Should be able to access by column name
    row = rows[0]
    assert "name" in row.keys()  # noqa: SIM118
    assert row["name"] == "Alice"

    # Should be able to iterate over keys
    keys = list(row.keys())
    assert "id" in keys
    assert "name" in keys
    assert "email" in keys
    assert "age" in keys


def test_rowguid_column_detection(rowguid_db: Connection) -> None:
    """Test detection of RowGUID columns (critical for DPM databases)."""
    inspector = Inspector(rowguid_db)
    rows = list(inspector.rows("entities"))

    # Verify RowGUID column exists and is accessible
    assert len(rows) == 2

    row = rows[0]
    assert "RowGUID" in row.keys()  # noqa: SIM118
    assert row["RowGUID"] == "guid-001"

    # Verify we can access RowGUID from any row
    for test_row in rows:
        assert test_row["RowGUID"] is not None
        assert isinstance(test_row["RowGUID"], str)
        assert test_row["RowGUID"].startswith("guid-")


def test_composite_primary_keys(composite_pk_db: Connection) -> None:
    """Test tables with composite primary keys."""
    inspector = Inspector(composite_pk_db)
    pks = list(inspector.pks("mappings"))

    # Should return all primary key columns in order
    assert pks == ["source_id", "target_id", "mapping_type"]

    # Verify we can access all PK columns from rows
    rows = list(inspector.rows("mappings"))
    assert len(rows) == 3

    for row in rows:
        assert row["source_id"] is not None
        assert row["target_id"] is not None
        assert row["mapping_type"] is not None
