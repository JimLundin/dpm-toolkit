"""Debug ordering-specific RowGUID matching."""

import tempfile
from pathlib import Path
from sqlite3 import connect

from compare.main import compare_databases


def test_ordering_impact() -> None:
    """Test how sort order affects RowGUID matching."""
    with tempfile.TemporaryDirectory() as temp_dir:
        print("=== Test 1: alpha before beta ===")
        old_db = Path(temp_dir) / "test1_old.db"
        new_db = Path(temp_dir) / "test1_new.db"

        # Create old database
        old_conn = connect(old_db)
        old_conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)",
        )
        old_conn.execute("INSERT INTO test VALUES (1, 'guid-alpha', 'Item A')")
        old_conn.commit()
        old_conn.close()

        # Create new database
        new_conn = connect(new_db)
        new_conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)",
        )
        new_conn.execute(
            "INSERT INTO test VALUES (10, 'guid-alpha', 'Item A Modified')",
        )
        new_conn.commit()
        new_conn.close()

        changes = list(next(iter(compare_databases(old_db, new_db))).body.rows.changes)
        modified = [c for c in changes if c.old and c.new]
        print(
            f"Single row test: Modified={len(modified)}, Total changes={len(changes)}",
        )

        print("\n=== Test 2: Two rows, both should match by RowGUID ===")
        old_db2 = Path(temp_dir) / "test2_old.db"
        new_db2 = Path(temp_dir) / "test2_new.db"

        # Create old database
        old_conn = connect(old_db2)
        old_conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)",
        )
        old_conn.executemany(
            "INSERT INTO test VALUES (?, ?, ?)",
            [
                (1, "guid-alpha", "Item A"),
                (2, "guid-beta", "Item B"),
            ],
        )
        old_conn.commit()
        old_conn.close()

        # Create new database
        new_conn = connect(new_db2)
        new_conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, RowGUID TEXT, name TEXT)",
        )
        new_conn.executemany(
            "INSERT INTO test VALUES (?, ?, ?)",
            [
                (10, "guid-alpha", "Item A Modified"),
                (20, "guid-beta", "Item B Modified"),
            ],
        )
        new_conn.commit()
        new_conn.close()

        changes = list(
            next(iter(compare_databases(old_db2, new_db2))).body.rows.changes,
        )
        modified = [c for c in changes if c.old and c.new]
        added = [c for c in changes if c.new and not c.old]
        removed = [c for c in changes if c.old and not c.new]

        print(
            f"""Two row test:
            Modified={len(modified)},
            Added={len(added)},
            Removed={len(removed)}""",
        )
        for i, change in enumerate(changes):
            old_data = dict(change.old) if change.old else None
            new_data = dict(change.new) if change.new else None
            print(f"  Change {i}: old={old_data}, new={new_data}")


if __name__ == "__main__":
    test_ordering_impact()
