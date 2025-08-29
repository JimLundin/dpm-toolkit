"""Debug ordering-specific RowGUID matching."""

from sqlite3 import connect

from compare.main import compare_databases


def test_ordering_impact() -> None:
    """Test how sort order affects RowGUID matching."""
    print("=== Test 1: alpha before beta ===")
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)"
    )
    old_conn.execute("INSERT INTO test VALUES (1, 'guid-alpha', 'Item A')")
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)"
    )
    new_conn.execute("INSERT INTO test VALUES (10, 'guid-alpha', 'Item A Modified')")
    new_conn.commit()

    changes = list(next(iter(compare_databases(old_conn, new_conn))).body.rows.changes)
    modified = [c for c in changes if c.old and c.new]
    print(f"Single row test: Modified={len(modified)}, Total changes={len(changes)}")

    print("\n=== Test 2: Two rows, both should match by RowGUID ===")
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)"
    )
    old_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            (1, "guid-alpha", "Item A"),
            (2, "guid-beta", "Item B"),
        ],
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)"
    )
    new_conn.executemany(
        "INSERT INTO test VALUES (?, ?, ?)",
        [
            (10, "guid-alpha", "Item A Modified"),
            (20, "guid-beta", "Item B Modified"),
        ],
    )
    new_conn.commit()

    changes = list(next(iter(compare_databases(old_conn, new_conn))).body.rows.changes)
    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    print(
        f"Two row test: Modified={len(modified)}, Added={len(added)}, Removed={len(removed)}"
    )
    for i, change in enumerate(changes):
        old_data = dict(change.old) if change.old else None
        new_data = dict(change.new) if change.new else None
        print(f"  Change {i}: old={old_data}, new={new_data}")


if __name__ == "__main__":
    test_ordering_impact()
