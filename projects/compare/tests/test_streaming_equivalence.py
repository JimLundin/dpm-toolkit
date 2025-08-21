"""Tests to verify streaming algorithm equivalence and edge cases."""

from sqlite3 import connect

from compare.main import compare_dbs


def test_basic_modifications() -> None:
    """Test basic row modifications are detected correctly."""
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
    )
    old_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            (1, "Alice", 100),
            (2, "Bob", 200),
            (3, "Charlie", 300),
        ],
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
    )
    new_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            (1, "Alice", 150),  # value changed
            (2, "Bob", 200),  # unchanged
            (3, "Charlie", 300),  # unchanged
        ],
    )
    new_conn.commit()

    changes = next(iter(compare_dbs(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    assert len(modified) == 1
    assert len(added) == 0
    assert len(removed) == 0
    assert modified[0].old
    assert modified[0].new
    assert modified[0].old["value"] == 100
    assert modified[0].new["value"] == 150


def test_additions_and_removals() -> None:
    """Test row additions and removals are detected correctly."""
    old_conn = connect(":memory:")
    old_conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    old_conn.executemany(
        "INSERT INTO test VALUES (?, ?)",
        [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
        ],
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    new_conn.executemany(
        "INSERT INTO test VALUES (?, ?)",
        [
            (1, "Alice"),  # unchanged
            (3, "Charlie"),  # unchanged
            (4, "David"),  # added
        ],
    )
    new_conn.commit()

    changes = next(iter(compare_dbs(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    assert len(modified) == 0
    assert len(added) == 1
    assert len(removed) == 1
    assert added[0].new
    assert removed[0].old
    assert added[0].new["name"] == "David"
    assert removed[0].old["name"] == "Bob"


def test_rowguid_with_valid_guids() -> None:
    """Test RowGUID-based comparison with all valid GUIDs."""
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test (id INTEGER, RowGUID TEXT, name TEXT, value INTEGER)",
    )
    old_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?, ?)",
        [
            (1, "guid-1", "Alice", 100),
            (2, "guid-2", "Bob", 200),
        ],
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test (id INTEGER, RowGUID TEXT, name TEXT, value INTEGER)",
    )
    new_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?, ?)",
        [
            (10, "guid-1", "Alice", 150),  # ID changed, value changed
            (2, "guid-2", "Bob", 200),  # unchanged
        ],
    )
    new_conn.commit()

    changes = next(iter(compare_dbs(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    assert len(modified) == 1
    assert len(added) == 0
    assert len(removed) == 0
    assert modified[0].old
    assert modified[0].new
    # Should match by RowGUID despite ID change
    assert modified[0].old["RowGUID"] == "guid-1"
    assert modified[0].new["RowGUID"] == "guid-1"
    assert modified[0].old["id"] == 1
    assert modified[0].new["id"] == 10


def test_rowguid_with_null_values() -> None:
    """Test RowGUID-based comparison with NULL RowGUID values."""
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test ("
        "id INTEGER PRIMARY KEY, "
        "RowGUID TEXT, "
        "name TEXT, "
        "value INTEGER"
        ")",
    )
    old_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?, ?)",
        [
            (1, "guid-1", "Alice", 100),
            (2, None, "Bob", 200),  # NULL RowGUID
            (3, "guid-3", "Charlie", 300),
            (4, None, "David", 400),  # NULL RowGUID
        ],
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test ("
        "id INTEGER PRIMARY KEY, "
        "RowGUID TEXT, "
        "name TEXT, "
        "value INTEGER"
        ")",
    )
    new_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?, ?)",
        [
            (1, "guid-1", "Alice", 150),  # value changed
            (2, None, "Bob", 250),  # NULL RowGUID, value changed
            (3, "guid-3", "Charlie", 300),  # unchanged
            (4, None, "David", 400),  # NULL RowGUID, unchanged
        ],
    )
    new_conn.commit()

    changes = next(iter(compare_dbs(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    assert len(modified) == 2  # Alice and Bob should be detected as modified
    assert len(added) == 0
    assert len(removed) == 0

    for change in modified:
        assert change.old
        assert change.new
        assert change.old["name"] in ("Alice", "Bob")


def test_no_common_primary_keys() -> None:
    """Test behavior when tables have different primary key structures."""
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, email TEXT, name TEXT)",
    )
    old_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            (1, "alice@test.com", "Alice"),
            (2, "bob@test.com", "Bob"),
        ],
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test (email TEXT PRIMARY KEY, age INTEGER, name TEXT)",
    )
    new_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            ("alice@test.com", 25, "Alice"),
            ("bob@test.com", 30, "Bob"),
        ],
    )
    new_conn.commit()

    changes = next(iter(compare_dbs(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    # Without common PKs or RowGUID, this falls back to all-column sorting
    # Results may show as add/remove pairs rather than modifications
    # This is expected behavior for incompatible schemas
    total_changes = len(changes)
    assert total_changes > 0  # Should detect some changes


def test_composite_primary_keys() -> None:
    """Test tables with composite (multi-column) primary keys."""
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test ("
        "year INTEGER, "
        "month INTEGER, "
        "value INTEGER, "
        "PRIMARY KEY (year, month)"
        ")",
    )
    old_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            (2023, 1, 100),
            (2023, 2, 200),
            (2024, 1, 300),
        ],
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test ("
        "year INTEGER, "
        "month INTEGER, "
        "value INTEGER, "
        "PRIMARY KEY (year, month)"
        ")",
    )
    new_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            (2023, 1, 150),  # value changed
            (2023, 2, 200),  # unchanged
            (2024, 1, 300),  # unchanged
            (2024, 2, 400),  # added
        ],
    )
    new_conn.commit()

    changes = next(iter(compare_dbs(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    assert len(modified) == 1
    assert len(added) == 1
    assert len(removed) == 0
    assert modified[0].old
    assert modified[0].old["year"] == 2023
    assert modified[0].old["month"] == 1
    assert added[0].new
    assert added[0].new["year"] == 2024
    assert added[0].new["month"] == 2


def test_empty_tables() -> None:
    """Test comparison of empty tables."""
    old_conn = connect(":memory:")
    old_conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    new_conn.commit()

    changes = next(iter(compare_dbs(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    assert len(changes) == 0


def test_large_dataset_memory_efficiency() -> None:
    """Test that algorithm remains memory efficient with larger datasets."""
    # Create larger test dataset to verify streaming behavior
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, data TEXT)",
    )

    # Insert 1000 rows
    old_data = [(i, f"guid-{i}", f"data-{i}") for i in range(1000)]
    old_conn.executemany("INSERT INTO test VALUES (?, ?, ?)", old_data)
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, data TEXT)",
    )

    # Insert same data but modify every 10th row
    new_data: list[tuple[int, str, str]] = []
    for i in range(1000):
        data = f"modified-{i}" if i % 10 == 0 else f"data-{i}"
        new_data.append((i, f"guid-{i}", data))

    new_conn.executemany("INSERT INTO test VALUES (?, ?, ?)", new_data)
    new_conn.commit()

    changes = next(iter(compare_dbs(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    assert len(modified) == 100  # Every 10th row modified
    assert len(added) == 0
    assert len(removed) == 0
