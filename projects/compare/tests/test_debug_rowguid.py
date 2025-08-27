"""Debug test to understand RowGUID matching behavior."""

from sqlite3 import connect

from compare.main import compare_databases


def test_simple_rowguid_different_pk() -> None:
    """Simple test with just 2 rows to debug the matching."""
    # Old database
    old_conn = connect(":memory:")
    old_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)"
    )
    old_conn.execute("INSERT INTO test VALUES (1, 'guid-a', 'Item A')")
    old_conn.commit()

    # New database
    new_conn = connect(":memory:")
    new_conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)"
    )
    new_conn.execute(
        "INSERT INTO test VALUES (10, 'guid-a', 'Item A Modified')"
    )  # Same RowGUID, different PK
    new_conn.commit()

    changes = next(iter(compare_databases(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    print("Changes found:")
    for i, change in enumerate(changes):
        print(
            f"  {i}: old={dict(change.old) if change.old else None}, new={dict(change.new) if change.new else None}"
        )

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    print(f"Modified: {len(modified)}, Added: {len(added)}, Removed: {len(removed)}")

    # Should be 1 modification
    assert len(modified) == 1, f"Expected 1 modification, got {len(modified)}"


if __name__ == "__main__":
    test_simple_rowguid_different_pk()
    print("✅ Debug test passed!")
