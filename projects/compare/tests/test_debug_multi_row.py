"""Debug multi-row RowGUID matching scenarios."""

import tempfile
from pathlib import Path
from sqlite3 import connect

from compare.main import compare_databases


def test_three_row_scenario() -> None:
    """Debug the 3-row scenario to see which matches are working."""
    with tempfile.TemporaryDirectory() as temp_dir:
        old_db = Path(temp_dir) / "old.db"
        new_db = Path(temp_dir) / "new.db"

        # Create old database
        old_conn = connect(old_db)
        old_conn.execute(
            """CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            RowGUID TEXT,
            name TEXT,
            category TEXT
            )""",
        )
        old_conn.executemany(
            "INSERT INTO products VALUES (?, ?, ?, ?)",
            [
                (1, "guid-alpha", "Product Alpha", "Electronics"),
                (2, "guid-beta", "Product Beta", "Books"),
                (3, "guid-gamma", "Product Gamma", "Electronics"),
            ],
        )
        old_conn.commit()
        old_conn.close()

        # Create new database
        new_conn = connect(new_db)
        new_conn.execute(
            """CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            RowGUID TEXT,
            name TEXT,
            category TEXT
            )""",
        )
        new_conn.executemany(
            "INSERT INTO products VALUES (?, ?, ?, ?)",
            [
                (
                    10,
                    "guid-alpha",
                    "Product Alpha Updated",
                    "Electronics",
                ),  # Different PK, RowGUID match
                (
                    2,
                    "guid-beta",
                    "Product Beta",
                    "Books",
                ),  # Same PK, RowGUID match, no change
                (
                    30,
                    "guid-gamma",
                    "Product Gamma",
                    "Home",
                ),  # Different PK, RowGUID match
            ],
        )
        new_conn.commit()
        new_conn.close()

        changes = next(iter(compare_databases(old_db, new_db))).body.rows.changes
        changes = list(changes)

    print("All changes:")
    for i, change in enumerate(changes):
        old_data = dict(change.old) if change.old else None
        new_data = dict(change.new) if change.new else None
        print(f"  {i}: old={old_data}")
        print(f"      new={new_data}")
        print()

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    print(
        f"""Summary:
        Modified={len(modified)},
        Added={len(added)},
        Removed={len(removed)}""",
    )

    for i, mod in enumerate(modified):
        print(
            f"Modification {i}: {mod.old['RowGUID']}"
            f"(id: {mod.old['id']} -> {mod.new['id']})",
        )


if __name__ == "__main__":
    test_three_row_scenario()
