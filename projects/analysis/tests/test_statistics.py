"""Tests for statistics collection."""

from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.orm import declarative_base

from analysis.statistics import StatisticsCollector

Base = declarative_base()


class SampleTable(Base):
    """Sample table for statistics collection tests."""

    __tablename__ = "test_table"

    id = Column(Integer, primary_key=True)
    status = Column(String)
    category = Column(String)
    score = Column(Integer)


def test_basic_statistics_collection() -> None:
    """Test basic statistics collection with in-memory database."""
    # Create in-memory database
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    # Insert test data
    with engine.connect() as conn:
        conn.execute(
            SampleTable.__table__.insert(),
            [
                {"id": 1, "status": "active", "category": "A", "score": 100},
                {"id": 2, "status": "active", "category": "B", "score": 200},
                {"id": 3, "status": "inactive", "category": "A", "score": 150},
                {"id": 4, "status": "active", "category": None, "score": 175},
                {"id": 5, "status": "pending", "category": "C", "score": None},
            ],
        )
        conn.commit()

    # Collect statistics
    collector = StatisticsCollector(engine)
    stats = collector.collect_table_statistics("test_table")

    # Verify basic counts
    assert "id" in stats
    assert "status" in stats
    assert "category" in stats
    assert "score" in stats

    # Check total rows
    assert stats["id"].total_rows == 5
    assert stats["status"].total_rows == 5
    assert stats["category"].total_rows == 5
    assert stats["score"].total_rows == 5

    # Check null counts
    assert stats["id"].null_count == 0
    assert stats["status"].null_count == 0
    assert stats["category"].null_count == 1  # One NULL
    assert stats["score"].null_count == 1  # One NULL

    # Check unique counts
    assert stats["id"].unique_count == 5
    assert stats["status"].unique_count == 3  # active, inactive, pending
    assert stats["category"].unique_count == 3  # A, B, C (NULL not counted)
    assert stats["score"].unique_count == 4  # 100, 150, 175, 200

    # Check value counts
    assert stats["status"].value_counts == {"active": 3, "inactive": 1, "pending": 1}
    assert stats["category"].value_counts == {"A": 2, "B": 1, "C": 1}

    # Check samples are collected
    assert len(stats["status"].value_samples) > 0
    assert len(stats["category"].value_samples) > 0


def test_large_cardinality_value_counts() -> None:
    """Test that value counts are not collected for high-cardinality columns."""
    # Create in-memory database
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    # Insert test data with high cardinality (more than MAX_VALUE_COUNTS)
    with engine.connect() as conn:
        conn.execute(
            SampleTable.__table__.insert(),
            [{"id": i, "status": f"status_{i}", "category": "A", "score": i}
             for i in range(1, 150)],  # 149 rows, each with unique status
        )
        conn.commit()

    # Collect statistics
    collector = StatisticsCollector(engine)
    stats = collector.collect_table_statistics("test_table")

    # Value counts should not be collected for high-cardinality status column
    # (unique_count = 149 > MAX_VALUE_COUNTS = 100)
    assert stats["status"].unique_count == 149
    assert stats["status"].value_counts == {}  # Should be empty

    # Value counts should be collected for low-cardinality category column
    assert stats["category"].unique_count == 1
    assert stats["category"].value_counts == {"A": 149}


def test_pattern_matching() -> None:
    """Test that pattern matching is performed during collection."""
    # Create in-memory database with UUID-like values
    engine = create_engine("sqlite:///:memory:")

    # Create a simple table
    with engine.connect() as conn:
        conn.execute(
            text("CREATE TABLE uuid_test (id INTEGER PRIMARY KEY, guid TEXT)"),
        )
        conn.execute(
            text(
                "INSERT INTO uuid_test (id, guid) VALUES "
                "(1, '550e8400-e29b-41d4-a716-446655440000'), "
                "(2, '6ba7b810-9dad-11d1-80b4-00c04fd430c8'), "
                "(3, '6ba7b811-9dad-11d1-80b4-00c04fd430c8')",
            ),
        )
        conn.commit()

    # Collect statistics
    collector = StatisticsCollector(engine)
    stats = collector.collect_table_statistics("uuid_test")

    # Check that UUID patterns were detected
    assert "guid" in stats
    assert stats["guid"].uuid_pattern_matches > 0
