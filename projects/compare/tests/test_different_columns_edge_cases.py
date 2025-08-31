"""Test edge cases when comparing tables with different column structures."""

import tempfile
from pathlib import Path
from sqlite3 import connect

from compare.main import compare_databases


def test_tables_with_added_columns() -> None:
    """Test when new table has additional columns."""
    with tempfile.TemporaryDirectory() as temp_dir:
        old_db = Path(temp_dir) / "old.db"
        new_db = Path(temp_dir) / "new.db"

        # Create old database with basic columns
        old_conn = connect(old_db)
        old_conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
        )
        old_conn.executemany(
            "INSERT INTO users VALUES (?, ?, ?)",
            [
                (1, "Alice", "alice@test.com"),
                (2, "Bob", "bob@test.com"),
            ],
        )
        old_conn.commit()
        old_conn.close()

        # Create new database with additional columns
        new_conn = connect(new_db)
        new_conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, city TEXT)"
        )
        new_conn.executemany(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
            [
                (1, "Alice", "alice@test.com", 30, "New York"),  # same data in common columns
                (2, "Bob", "bob@test.com", 25, "Boston"),  # same data in common columns
                (3, "Charlie", "charlie@test.com", 35, "Chicago"),  # new row
            ],
        )
        new_conn.commit()
        new_conn.close()

        comparison = next(iter(compare_databases(old_db, new_db)))
        
        # Check schema changes - should detect added columns
        schema_changes = list(comparison.body.columns.changes)
        added_columns = [c for c in schema_changes if c.new and not c.old]
        
        assert len(added_columns) == 2, f"Expected 2 added columns, got {len(added_columns)}"
        added_column_names = {c.new["name"] for c in added_columns}
        assert added_column_names == {"age", "city"}

        # Check row changes - existing rows detected as modified due to structural changes
        row_changes = list(comparison.body.rows.changes)
        added_rows = [c for c in row_changes if c.new and not c.old]
        modified_rows = [c for c in row_changes if c.old and c.new]
        
        assert len(added_rows) == 1, f"Expected 1 added row, got {len(added_rows)}"
        assert len(modified_rows) == 2, f"Expected 2 modified rows, got {len(modified_rows)}"
        assert added_rows[0].new["name"] == "Charlie"
        
        # Verify Alice and Bob are detected as modified due to structural differences
        modified_names = {c.new["name"] for c in modified_rows}
        assert modified_names == {"Alice", "Bob"}


def test_tables_with_removed_columns() -> None:
    """Test when old table has columns that new table lacks."""
    with tempfile.TemporaryDirectory() as temp_dir:
        old_db = Path(temp_dir) / "old.db"
        new_db = Path(temp_dir) / "new.db"

        # Create old database with more columns
        old_conn = connect(old_db)
        old_conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, phone TEXT, address TEXT)"
        )
        old_conn.executemany(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
            [
                (1, "Alice", "alice@test.com", "555-1234", "123 Main St"),
                (2, "Bob", "bob@test.com", "555-5678", "456 Oak Ave"),
            ],
        )
        old_conn.commit()
        old_conn.close()

        # Create new database with fewer columns
        new_conn = connect(new_db)
        new_conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
        )
        new_conn.executemany(
            "INSERT INTO users VALUES (?, ?, ?)",
            [
                (1, "Alice", "alice@test.com"),  # same data in common columns
                (2, "Bob Updated", "bob@test.com"),  # name changed
            ],
        )
        new_conn.commit()
        new_conn.close()

        comparison = next(iter(compare_databases(old_db, new_db)))
        
        # Check schema changes - should detect removed columns
        schema_changes = list(comparison.body.columns.changes)
        removed_columns = [c for c in schema_changes if c.old and not c.new]
        
        assert len(removed_columns) == 2, f"Expected 2 removed columns, got {len(removed_columns)}"
        removed_column_names = {c.old["name"] for c in removed_columns}
        assert removed_column_names == {"phone", "address"}

        # Check row changes - both rows modified due to structural changes
        row_changes = list(comparison.body.rows.changes)
        modified_rows = [c for c in row_changes if c.old and c.new]
        
        assert len(modified_rows) == 2, f"Expected 2 modified rows, got {len(modified_rows)}"
        
        # Find Bob specifically to verify the name change
        bob_change = next((c for c in modified_rows if c.new["name"].startswith("Bob")), None)
        assert bob_change is not None, "Should find Bob's change"
        assert bob_change.old["name"] == "Bob"
        assert bob_change.new["name"] == "Bob Updated"
        
        # Verify Alice is also detected as modified due to structural differences
        alice_change = next((c for c in modified_rows if c.new["name"] == "Alice"), None)
        assert alice_change is not None, "Should find Alice's change"


def test_tables_with_column_type_changes() -> None:
    """Test when columns exist in both tables but with different types."""
    with tempfile.TemporaryDirectory() as temp_dir:
        old_db = Path(temp_dir) / "old.db"
        new_db = Path(temp_dir) / "new.db"

        # Create old database 
        old_conn = connect(old_db)
        old_conn.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)"  # price as INTEGER
        )
        old_conn.executemany(
            "INSERT INTO products VALUES (?, ?, ?)",
            [
                (1, "Widget", 100),
                (2, "Gadget", 200),
            ],
        )
        old_conn.commit()
        old_conn.close()

        # Create new database with different column type
        new_conn = connect(new_db)
        new_conn.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)"  # price as REAL
        )
        new_conn.executemany(
            "INSERT INTO products VALUES (?, ?, ?)",
            [
                (1, "Widget", 99.99),  # changed from 100
                (2, "Gadget", 200.0),  # same value, different type
            ],
        )
        new_conn.commit()
        new_conn.close()

        comparison = next(iter(compare_databases(old_db, new_db)))
        
        # Check schema changes - should detect type change
        schema_changes = list(comparison.body.columns.changes)
        modified_columns = [c for c in schema_changes if c.old and c.new]
        
        price_change = None
        for c in modified_columns:
            if c.old["name"] == "price":
                price_change = c
                break
                
        assert price_change is not None, "Expected to find price column type change"
        assert price_change.old["type"] == "INTEGER"
        assert price_change.new["type"] == "REAL"

        # Check row changes - should detect the price change for Widget
        row_changes = list(comparison.body.rows.changes)
        modified_rows = [c for c in row_changes if c.old and c.new]
        
        # Widget should be detected as modified due to price change
        widget_change = None
        for c in modified_rows:
            if c.old["name"] == "Widget":
                widget_change = c
                break
                
        assert widget_change is not None, "Expected Widget to be detected as modified"
        assert widget_change.old["price"] == 100
        assert widget_change.new["price"] == 99.99


def test_completely_different_columns() -> None:
    """Test tables with completely different column sets."""
    with tempfile.TemporaryDirectory() as temp_dir:
        old_db = Path(temp_dir) / "old.db"
        new_db = Path(temp_dir) / "new.db"

        # Create old database with one set of columns
        old_conn = connect(old_db)
        old_conn.execute(
            "CREATE TABLE data (id INTEGER PRIMARY KEY, old_col1 TEXT, old_col2 INTEGER)"
        )
        old_conn.executemany(
            "INSERT INTO data VALUES (?, ?, ?)",
            [
                (1, "old_data1", 10),
                (2, "old_data2", 20),
            ],
        )
        old_conn.commit()
        old_conn.close()

        # Create new database with completely different columns (except id)
        new_conn = connect(new_db)
        new_conn.execute(
            "CREATE TABLE data (id INTEGER PRIMARY KEY, new_col1 TEXT, new_col2 REAL)"
        )
        new_conn.executemany(
            "INSERT INTO data VALUES (?, ?, ?)",
            [
                (1, "new_data1", 1.5),  # same id, different data
                (3, "new_data3", 3.5),  # different id
            ],
        )
        new_conn.commit()
        new_conn.close()

        comparison = next(iter(compare_databases(old_db, new_db)))
        
        # Check schema changes
        schema_changes = list(comparison.body.columns.changes)
        added_columns = [c for c in schema_changes if c.new and not c.old]
        removed_columns = [c for c in schema_changes if c.old and not c.new]
        
        assert len(added_columns) == 2  # new_col1, new_col2
        assert len(removed_columns) == 2  # old_col1, old_col2
        
        # Check row changes - only id is common, so comparison should work by PK
        row_changes = list(comparison.body.rows.changes)
        
        # Should detect:
        # - Row 1: exists in both (matched by id PK) but no common non-PK columns to compare
        # - Row 2: removed (exists in old, not in new)  
        # - Row 3: added (exists in new, not in old)
        added_rows = [c for c in row_changes if c.new and not c.old]
        removed_rows = [c for c in row_changes if c.old and not c.new]
        modified_rows = [c for c in row_changes if c.old and c.new]
        
        assert len(added_rows) == 1, f"Expected 1 added row, got {len(added_rows)}"
        assert len(removed_rows) == 1, f"Expected 1 removed row, got {len(removed_rows)}"
        # Row 1 should show as modified due to different structure
        assert len(modified_rows) == 1, f"Expected 1 modified row, got {len(modified_rows)}"
        
        # Verify it's row with id=1
        assert modified_rows[0].old["id"] == 1
        assert modified_rows[0].new["id"] == 1


def test_rowguid_with_different_columns() -> None:
    """Test RowGUID matching when tables have different column structures."""
    with tempfile.TemporaryDirectory() as temp_dir:
        old_db = Path(temp_dir) / "old.db"
        new_db = Path(temp_dir) / "new.db"

        # Create old database
        old_conn = connect(old_db)
        old_conn.execute(
            "CREATE TABLE entities (id INTEGER PRIMARY KEY, RowGUID TEXT, old_data TEXT, shared_data TEXT)"
        )
        old_conn.executemany(
            "INSERT INTO entities VALUES (?, ?, ?, ?)",
            [
                (1, "guid-1", "old1", "shared1"),
                (2, "guid-2", "old2", "shared2"),
            ],
        )
        old_conn.commit()
        old_conn.close()

        # Create new database with different PK and additional columns
        new_conn = connect(new_db)
        new_conn.execute(
            "CREATE TABLE entities (new_id INTEGER PRIMARY KEY, RowGUID TEXT, new_data TEXT, shared_data TEXT)"
        )
        new_conn.executemany(
            "INSERT INTO entities VALUES (?, ?, ?, ?)",
            [
                (100, "guid-1", "new1", "shared1_updated"),  # RowGUID match, shared_data changed
                (200, "guid-2", "new2", "shared2"),  # RowGUID match, no change in shared columns
            ],
        )
        new_conn.commit()
        new_conn.close()

        comparison = next(iter(compare_databases(old_db, new_db)))
        
        # Check schema changes
        schema_changes = list(comparison.body.columns.changes)
        added_columns = [c for c in schema_changes if c.new and not c.old]
        removed_columns = [c for c in schema_changes if c.old and not c.new]
        
        assert len(added_columns) == 2  # new_id, new_data
        assert len(removed_columns) == 2  # id, old_data

        # Check row changes - should match by RowGUID despite different PKs
        row_changes = list(comparison.body.rows.changes)
        modified_rows = [c for c in row_changes if c.old and c.new]
        
        # Should detect two modifications due to structural changes
        assert len(modified_rows) == 2, f"Expected 2 modified rows, got {len(modified_rows)}"
        
        # Find the entity with actual data change
        guid1_change = next((c for c in modified_rows if c.old["RowGUID"] == "guid-1"), None)
        assert guid1_change is not None, "Should find guid-1 change"
        assert guid1_change.old["shared_data"] == "shared1"
        assert guid1_change.new["shared_data"] == "shared1_updated"  # change detected
        
        # Find the entity with only structural change
        guid2_change = next((c for c in modified_rows if c.old["RowGUID"] == "guid-2"), None)  
        assert guid2_change is not None, "Should find guid-2 change"
        assert guid2_change.old["shared_data"] == "shared2"
        assert guid2_change.new["shared_data"] == "shared2"  # no data change, just structural


def test_no_common_columns_except_pk() -> None:
    """Test when tables share only primary key columns."""
    with tempfile.TemporaryDirectory() as temp_dir:
        old_db = Path(temp_dir) / "old.db"
        new_db = Path(temp_dir) / "new.db"

        # Create old database
        old_conn = connect(old_db)
        old_conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, old_only TEXT)"
        )
        old_conn.executemany(
            "INSERT INTO test VALUES (?, ?)",
            [
                (1, "old_value1"),
                (2, "old_value2"),
            ],
        )
        old_conn.commit()
        old_conn.close()

        # Create new database with only PK in common
        new_conn = connect(new_db)
        new_conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, new_only TEXT)"
        )
        new_conn.executemany(
            "INSERT INTO test VALUES (?, ?)",
            [
                (1, "new_value1"),  # same PK
                (3, "new_value3"),  # different PK
            ],
        )
        new_conn.commit()
        new_conn.close()

        comparison = next(iter(compare_databases(old_db, new_db)))
        
        # Check row changes
        row_changes = list(comparison.body.rows.changes)
        added_rows = [c for c in row_changes if c.new and not c.old]
        removed_rows = [c for c in row_changes if c.old and not c.new]
        modified_rows = [c for c in row_changes if c.old and c.new]
        
        assert len(added_rows) == 1, f"Expected 1 added row, got {len(added_rows)}"
        assert len(removed_rows) == 1, f"Expected 1 removed row, got {len(removed_rows)}"
        # Row with id=1 exists in both but has different structure,
        # so it should be flagged as modified
        assert len(modified_rows) == 1, f"Expected 1 modified row, got {len(modified_rows)}"
        
        # Verify it's the row with id=1
        assert modified_rows[0].old["id"] == 1
        assert modified_rows[0].new["id"] == 1


if __name__ == "__main__":
    test_tables_with_added_columns()
    test_tables_with_removed_columns()
    test_tables_with_column_type_changes()
    test_completely_different_columns()
    test_rowguid_with_different_columns()
    test_no_common_columns_except_pk()
    print("✅ All different columns edge case tests passed!")