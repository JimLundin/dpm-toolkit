"""Test specific edge case: NULL RowGUID handling with composite sort order."""

from sqlite3 import connect

from compare.main import compare_databases


def test_null_rowguid_composite_sort_order() -> None:
    """Test the specific edge case that motivated composite RowGUID + PK sorting.

    Before the fix: When RowGUID existed but some rows had NULL values,
    sorting by RowGUID alone caused incorrect matching.

    After the fix: Composite sort order (RowGUID, PK) ensures proper matching
    for both valid GUIDs and NULL GUIDs.
    """
    # Create scenario with mixed NULL/non-NULL RowGUIDs
    old_conn = connect(":memory:")
    old_conn.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            RowGUID TEXT,
            email TEXT UNIQUE,
            name TEXT,
            status TEXT
        )
    """,
    )
    old_conn.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
        [
            (1, "guid-alice", "alice@test.com", "Alice Smith", "active"),
            (2, None, "bob@test.com", "Bob Jones", "active"),  # NULL RowGUID
            (3, "guid-charlie", "charlie@test.com", "Charlie Brown", "active"),
            (4, None, "david@test.com", "David Wilson", "inactive"),  # NULL RowGUID
            (5, "guid-eve", "eve@test.com", "Eve Davis", "active"),
        ],
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            RowGUID TEXT,
            email TEXT UNIQUE,
            name TEXT,
            status TEXT
        )
    """,
    )
    new_conn.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
        [
            (
                1,
                "guid-alice",
                "alice@test.com",
                "Alice Smith",
                "inactive",
            ),  # status changed
            (
                2,
                None,
                "bob@test.com",
                "Bob Jones",
                "active",
            ),  # unchanged (NULL RowGUID)
            (
                3,
                "guid-charlie",
                "charlie@test.com",
                "Charlie Brown",
                "active",
            ),  # unchanged
            (
                4,
                None,
                "david@test.com",
                "David Wilson",
                "active",
            ),  # status changed (NULL RowGUID)
            (5, "guid-eve", "eve@test.com", "Eve Davis", "inactive"),  # status changed
            (
                6,
                None,
                "frank@test.com",
                "Frank Miller",
                "active",
            ),  # added (NULL RowGUID)
        ],
    )
    new_conn.commit()

    changes = next(iter(compare_databases(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    # Categorize changes
    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    # Should correctly identify 3 modifications and 1 addition
    assert len(modified) == 3, f"Expected 3 modifications, got {len(modified)}"
    assert len(added) == 1, f"Expected 1 addition, got {len(added)}"
    assert len(removed) == 0, f"Expected 0 removals, got {len(removed)}"

    # Verify specific modifications
    for change in modified:
        assert change.old
        assert change.new
        if change.old["name"] == "Alice Smith":
            assert change.old["RowGUID"] == "guid-alice"
            assert change.new["RowGUID"] == "guid-alice"
            assert change.old["status"] == "active"
            assert change.new["status"] == "inactive"
        elif change.old["name"] == "David Wilson":
            assert change.old["RowGUID"] is None
            assert change.new["RowGUID"] is None
            assert change.old["status"] == "inactive"
            assert change.new["status"] == "active"
        elif change.old["name"] == "Eve Davis":
            assert change.old["RowGUID"] == "guid-eve"
            assert change.new["RowGUID"] == "guid-eve"
            assert change.old["status"] == "active"
            assert change.new["status"] == "inactive"
        elif change.old["name"] == "Frank Miller":
            assert change.old["RowGUID"] is None
            assert change.new["RowGUID"] is None
            assert change.new["name"] == "Frank Miller"
            assert change.new["status"] == "active"
        else:
            msg = f"Unexpected change: {change}"
            raise ValueError(msg)
    # Alice: matched by RowGUID (status: active -> inactive)

    # Frank: new addition with NULL RowGUID
    assert len(added) == 1
    assert added[0].new
    assert added[0].new["name"] == "Frank Miller"
    assert added[0].new["RowGUID"] is None


def test_all_null_rowguids() -> None:
    """Test case where all rows have NULL RowGUIDs - should fall back to PK matching."""
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, data TEXT)",
    )
    old_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            (1, None, "data1"),
            (2, None, "data2"),
            (3, None, "data3"),
        ],
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, data TEXT)",
    )
    new_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            (1, None, "modified1"),  # data changed
            (2, None, "data2"),  # unchanged
            (3, None, "data3"),  # unchanged
            (4, None, "data4"),  # added
        ],
    )
    new_conn.commit()

    changes = next(iter(compare_databases(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    # Should correctly match by PK despite all NULL RowGUIDs
    assert len(modified) == 1
    assert len(added) == 1
    assert len(removed) == 0

    assert modified[0].old
    assert modified[0].new
    assert modified[0].old["id"] == 1
    assert modified[0].old["data"] == "data1"
    assert modified[0].new["data"] == "modified1"


if __name__ == "__main__":
    test_null_rowguid_composite_sort_order()
    test_all_null_rowguids()
    print("✅ All NULL RowGUID edge case tests passed!")
