"""Integration test that mimics migrate module output for enum detection."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from sqlite3 import connect

import pytest
from sqlalchemy import (
    Column,
    Enum,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
)

from schema.main import sqlite_to_schema
from schema.sqlalchemy_export import schema_to_sqlalchemy


@pytest.fixture(name="migrate_output_db")
def migrate_output_database() -> str:
    """Create a database that exactly mimics what the migrate module produces."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    # Create database using SQLAlchemy exactly like migrate module does
    engine = create_engine(f"sqlite:///{db_path}")
    metadata = MetaData()

    # This mimics the output of migrate module after enum processing
    # Column types are set to Enum(*values, create_constraint=True)
    test_table = Table(
        "TestData",
        metadata,
        Column("ID", Integer, primary_key=True),
        Column(
            "StatusType",
            Enum("active", "inactive", "pending", create_constraint=True),
        ),
        Column(
            "UserStatus",
            Enum("new", "verified", "suspended", create_constraint=True),
        ),
        Column(
            "DirectionSign",
            Enum("up", "down", "left", "right", create_constraint=True),
        ),
        Column("ErrorCode", Enum("E001", "E002", "E003", create_constraint=True)),
        Column("Description", String(100)),  # Not an enum
    )

    # Create a second table to test multiple table handling
    status_table = Table(
        "StatusLog",
        metadata,
        Column("LogID", Integer, primary_key=True),
        Column(
            "ChangeType",
            Enum("insert", "update", "delete", create_constraint=True),
        ),
        Column(
            "Severity",
            Enum("low", "medium", "high", "critical", create_constraint=True),
        ),
        Column("Message", String(255)),  # Not an enum
    )

    # Create tables - this should generate CHECK constraints
    metadata.create_all(engine)

    # Insert some test data to verify constraints work
    with engine.connect() as conn:
        conn.execute(
            test_table.insert().values(
                [
                    {
                        "ID": 1,
                        "StatusType": "active",
                        "UserStatus": "new",
                        "DirectionSign": "up",
                        "ErrorCode": "E001",
                        "Description": "Test 1",
                    },
                    {
                        "ID": 2,
                        "StatusType": "inactive",
                        "UserStatus": "verified",
                        "DirectionSign": "down",
                        "ErrorCode": "E002",
                        "Description": "Test 2",
                    },
                ],
            ),
        )

        conn.execute(
            status_table.insert().values(
                [
                    {
                        "LogID": 1,
                        "ChangeType": "insert",
                        "Severity": "low",
                        "Message": "Record created",
                    },
                    {
                        "LogID": 2,
                        "ChangeType": "update",
                        "Severity": "medium",
                        "Message": "Record updated",
                    },
                ],
            ),
        )

        conn.commit()

    return db_path


def test_migrate_output_has_check_constraints(migrate_output_db: str) -> None:
    """Verify that our mock migrate output actually creates CHECK constraints."""
    engine = create_engine(f"sqlite:///{migrate_output_db}")
    inspector = inspect(engine)

    # Check TestData table constraints
    test_data_constraints = inspector.get_check_constraints("TestData")
    assert (
        len(test_data_constraints) == 4
    ), f"Expected 4 constraints, got {len(test_data_constraints)}"

    # Verify specific constraint content
    constraint_texts = [c["sqltext"] for c in test_data_constraints]
    expected_constraints = [
        "\"StatusType\" IN ('active', 'inactive', 'pending')",
        "\"UserStatus\" IN ('new', 'verified', 'suspended')",
        "\"DirectionSign\" IN ('up', 'down', 'left', 'right')",
        "\"ErrorCode\" IN ('E001', 'E002', 'E003')",
    ]

    for expected in expected_constraints:
        assert any(
            expected in text for text in constraint_texts
        ), f"Missing constraint: {expected}"

    # Check StatusLog table constraints
    status_log_constraints = inspector.get_check_constraints("StatusLog")
    assert (
        len(status_log_constraints) == 2
    ), f"Expected 2 constraints, got {len(status_log_constraints)}"


def test_schema_generation_from_migrate_output(migrate_output_db: str) -> None:
    """Test that schema module correctly generates Literal types from migrate output."""
    engine = create_engine(f"sqlite:///{migrate_output_db}")
    schema = sqlite_to_schema(engine)
    schema_code = schema_to_sqlalchemy(schema)

    print("Generated schema code:")
    print(schema_code)

    # Verify Literal types are generated for all enum columns (values are sorted)
    expected_literals = [
        'Literal["active", "inactive", "pending"]',  # StatusType
        'Literal["new", "suspended", "verified"]',  # UserStatus (sorted)
        'Literal["down", "left", "right", "up"]',  # DirectionSign (sorted)
        'Literal["E001", "E002", "E003"]',  # ErrorCode
        'Literal["delete", "insert", "update"]',  # ChangeType (sorted)
        'Literal["critical", "high", "low", "medium"]',  # Severity (sorted)
    ]

    for expected_literal in expected_literals:
        assert (
            expected_literal in schema_code
        ), f"Missing literal type: {expected_literal}"

    # Verify non-enum columns are regular types
    assert "description: Mapped[str | None]" in schema_code
    assert "message: Mapped[str | None]" in schema_code
    assert (
        "id: Mapped[int]" in schema_code or "i_d: Mapped[int]" in schema_code
    )  # ID might be snake_cased

    # Verify table classes are generated
    assert "class TestData(DPM):" in schema_code
    assert "class StatusLog(DPM):" in schema_code

    # Count total Literal types to ensure we got them all
    literal_count = schema_code.count("Literal[")
    assert literal_count == 6, f"Expected 6 Literal types, found {literal_count}"


def test_raw_sql_verification(migrate_output_db: str) -> None:
    """Verify the raw SQL contains CHECK constraints like real migrate output."""
    conn = connect(migrate_output_db)
    cursor = conn.cursor()

    # Get table creation SQL
    cursor.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name",
    )
    tables = cursor.fetchall()

    # Verify TestData table has CHECK constraints in SQL
    test_data_sql = next((sql for name, sql in tables if name == "TestData"), None)
    assert test_data_sql is not None
    assert "CHECK" in test_data_sql
    assert "StatusType" in test_data_sql
    assert "active" in test_data_sql

    # Verify StatusLog table has CHECK constraints in SQL
    status_log_sql = next((sql for name, sql in tables if name == "StatusLog"), None)
    assert status_log_sql is not None
    assert "CHECK" in status_log_sql
    assert "ChangeType" in status_log_sql
    assert "insert" in status_log_sql

    conn.close()


def test_enum_detection_with_various_patterns(migrate_output_db: str) -> None:
    """Test that different enum value patterns are handled correctly."""
    # This verifies the enum detection works with:
    # - Mixed case values (E001, E002)
    # - Single words (active, inactive)
    # - Multi-character patterns (insert, update, delete)
    # - Directional values (up, down, left, right)

    engine = create_engine(f"sqlite:///{migrate_output_db}")
    schema = sqlite_to_schema(engine)
    schema_code = schema_to_sqlalchemy(schema)

    # Test mixed alphanumeric (error codes)
    assert '"E001"' in schema_code
    assert '"E002"' in schema_code
    assert '"E003"' in schema_code

    # Test action verbs
    assert '"insert"' in schema_code
    assert '"update"' in schema_code
    assert '"delete"' in schema_code

    # Test directional values
    assert '"up"' in schema_code
    assert '"down"' in schema_code
    assert '"left"' in schema_code
    assert '"right"' in schema_code


@pytest.fixture(autouse=True)
def cleanup_migrate_output_db(migrate_output_db: str) -> Generator[None]:
    """Clean up the test database after each test."""
    yield
    Path(migrate_output_db).unlink(missing_ok=True)
