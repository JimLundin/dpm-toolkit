"""Tests for statistics collection."""

import time

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
            [
                {"id": i, "status": f"status_{i}", "category": "A", "score": i}
                for i in range(1, 150)
            ],  # 149 rows, each with unique status
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


def test_sample_collection_performance() -> None:
    """Test that sample collection uses O(1) set lookup, not O(n) list scan."""
    # Create in-memory database with large dataset
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    # Insert many rows to test performance
    # With O(n) list lookup, this would be very slow
    # With O(1) set lookup, should be fast
    with engine.connect() as conn:
        # Insert 1000 rows (well within MAX_SAMPLE_ROWS = 10000)
        conn.execute(
            SampleTable.__table__.insert(),
            [
                {
                    "id": i,
                    "status": f"status_{i % 10}",  # 10 unique statuses
                    "category": f"cat_{i % 5}",  # 5 unique categories
                    "score": i,
                }
                for i in range(1, 1001)
            ],
        )
        conn.commit()

    # Collect statistics - should complete quickly
    collector = StatisticsCollector(engine)
    start = time.time()
    stats = collector.collect_table_statistics("test_table")
    elapsed = time.time() - start

    # Verify samples were collected correctly
    assert len(stats["status"].value_samples) <= 50  # MAX_SAMPLES
    assert len(stats["category"].value_samples) <= 50

    # With O(1) lookup, should complete in well under 1 second
    # With O(n) lookup, would take much longer
    assert elapsed < 1.0, f"Collection took {elapsed}s, expected < 1s"

    # Verify sample uniqueness (sets ensure no duplicates)
    status_samples = stats["status"].value_samples
    assert len(status_samples) == len(set(status_samples))

    category_samples = stats["category"].value_samples
    assert len(category_samples) == len(set(category_samples))


def test_sample_collection_respects_max_samples() -> None:
    """Test that sample collection doesn't exceed MAX_SAMPLES."""
    # Create database with many unique values
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        # Insert 100 rows with unique statuses
        conn.execute(
            SampleTable.__table__.insert(),
            [
                {
                    "id": i,
                    "status": f"unique_status_{i}",
                    "category": "A",
                    "score": i,
                }
                for i in range(1, 101)
            ],
        )
        conn.commit()

    collector = StatisticsCollector(engine)
    stats = collector.collect_table_statistics("test_table")

    # Should not exceed MAX_SAMPLES (50)
    assert len(stats["status"].value_samples) <= collector.MAX_SAMPLES
    assert len(stats["status"].value_samples) == 50  # Should be exactly 50


def test_deterministic_sampling() -> None:
    """Test that sampling produces consistent, reproducible results."""
    # Create database with consistent data
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        # Insert test data
        conn.execute(
            SampleTable.__table__.insert(),
            [
                {
                    "id": i,
                    "status": f"status_{i % 5}",
                    "category": f"cat_{i % 3}",
                    "score": i * 10,
                }
                for i in range(1, 101)
            ],
        )
        conn.commit()

    # Collect statistics twice
    collector1 = StatisticsCollector(engine)
    stats1 = collector1.collect_table_statistics("test_table")

    collector2 = StatisticsCollector(engine)
    stats2 = collector2.collect_table_statistics("test_table")

    # Results should be identical
    assert stats1["status"].value_samples == stats2["status"].value_samples
    assert stats1["category"].value_samples == stats2["category"].value_samples
    assert stats1["score"].value_samples == stats2["score"].value_samples


def test_sampling_without_primary_key() -> None:
    """Test that sampling works for tables without primary keys."""
    # Create table without primary key
    engine = create_engine("sqlite:///:memory:")

    with engine.connect() as conn:
        # Create table without PK
        conn.execute(
            text("CREATE TABLE no_pk_table (name TEXT, value INTEGER)"),
        )
        conn.execute(
            text(
                "INSERT INTO no_pk_table (name, value) VALUES "
                "('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5)",
            ),
        )
        conn.commit()

    # Should not crash - should fallback to first column ordering
    collector = StatisticsCollector(engine)
    stats = collector.collect_table_statistics("no_pk_table")

    # Verify statistics collected
    assert "name" in stats
    assert "value" in stats
    assert len(stats["name"].value_samples) > 0
    assert len(stats["value"].value_samples) > 0

    # Should still be deterministic
    collector2 = StatisticsCollector(engine)
    stats2 = collector2.collect_table_statistics("no_pk_table")

    assert stats["name"].value_samples == stats2["name"].value_samples
    assert stats["value"].value_samples == stats2["value"].value_samples


def test_single_query_optimization() -> None:
    """Test that basic statistics collection produces correct results.

    While we can't easily count queries without complex mocking,
    we verify the single-query implementation gives correct statistics.

    """
    # Create database
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(
            SampleTable.__table__.insert(),
            [
                {"id": 1, "status": "active", "category": "A", "score": 100},
                {"id": 2, "status": "active", "category": None, "score": 200},
                {"id": 3, "status": None, "category": "B", "score": None},
                {"id": 4, "status": "inactive", "category": "C", "score": 300},
            ],
        )
        conn.commit()

    collector = StatisticsCollector(engine)

    # Verify correct statistics from single-query implementation
    with collector.engine.connect() as conn:
        table = collector.metadata.tables["test_table"]
        stats = collector._collect_basic_statistics(conn, table)  # noqa: SLF001

    # Check total rows
    assert all(s.total_rows == 4 for s in stats.values())

    # Check null counts
    assert stats["id"].null_count == 0
    assert stats["status"].null_count == 1  # One NULL
    assert stats["category"].null_count == 1  # One NULL
    assert stats["score"].null_count == 1  # One NULL

    # Check unique counts
    assert stats["id"].unique_count == 4
    assert stats["status"].unique_count == 2  # active, inactive (NULL not counted)
    assert stats["category"].unique_count == 3  # A, B, C
    assert stats["score"].unique_count == 3  # 100, 200, 300


def test_statistics_with_many_columns() -> None:
    """Test that statistics collection scales well with many columns."""
    # Create table with many columns
    engine = create_engine("sqlite:///:memory:")

    with engine.connect() as conn:
        # Create table with 50 columns
        columns = ["id INTEGER PRIMARY KEY"]
        columns.extend([f"col{i} INTEGER" for i in range(1, 50)])
        create_sql = f"CREATE TABLE many_cols ({', '.join(columns)})"
        conn.execute(text(create_sql))

        # Insert some data using named parameters for text()
        param_names = ["id"] + [f"col{i}" for i in range(1, 50)]
        placeholders = ", ".join([f":{name}" for name in param_names])
        insert_sql = f"INSERT INTO many_cols VALUES ({placeholders})"  # noqa: S608

        for i in range(1, 101):
            params = {"id": i}
            params.update({f"col{j}": i * j for j in range(1, 50)})
            conn.execute(text(insert_sql), params)
        conn.commit()

    # Collect statistics - should be fast even with 50 columns
    collector = StatisticsCollector(engine)
    start = time.time()
    stats = collector.collect_table_statistics("many_cols")
    elapsed = time.time() - start

    # Verify all columns present
    assert len(stats) == 50
    assert all(f"col{i}" in stats for i in range(1, 50))

    # With single-query optimization, should complete quickly
    # Without optimization (101 queries), would be much slower
    assert elapsed < 2.0, f"Collection took {elapsed}s, expected < 2s"
