"""Test real-world case where rows with same RowGUID but different PKs should match."""

from sqlite3 import connect

from compare.main import compare_databases


def test_dora_module_case() -> None:
    """Test the actual DORA module case where same RowGUID rows should match despite different ModuleVID."""
    # Create old database with the problematic row structure
    old_conn = connect(":memory:")
    old_conn.execute(
        """
        CREATE TABLE modules (
            ModuleVID INTEGER PRIMARY KEY,
            ModuleID INTEGER,
            GlobalKeyID INTEGER,
            StartReleaseID INTEGER,
            EndReleaseID INTEGER,
            Code TEXT,
            Name TEXT,
            Description TEXT,
            VersionNumber TEXT,
            FromReferenceDate TEXT,
            ToReferenceDate TEXT,
            RowGUID TEXT,
            IsReported INTEGER,
            IsCalculated INTEGER
        )
        """,
    )
    old_conn.execute(
        """
        INSERT INTO modules VALUES (
            390, 44, NULL, 2, 3, 'DORA', 'DORA', NULL, '1.0.0', 
            '2025-01-31', '2025-01-31', 'c0a78332d1ffb8448495194dfce5efe2', 1, 0
        )
        """,
    )
    old_conn.commit()

    # Create new database with same data but different ModuleVID
    new_conn = connect(":memory:")
    new_conn.execute(
        """
        CREATE TABLE modules (
            ModuleVID INTEGER PRIMARY KEY,
            ModuleID INTEGER,
            GlobalKeyID INTEGER,
            StartReleaseID INTEGER,
            EndReleaseID INTEGER,
            Code TEXT,
            Name TEXT,
            Description TEXT,
            VersionNumber TEXT,
            FromReferenceDate TEXT,
            ToReferenceDate TEXT,
            RowGUID TEXT,
            IsReported INTEGER,
            IsCalculated INTEGER
        )
        """,
    )
    new_conn.execute(
        """
        INSERT INTO modules VALUES (
            391, 44, NULL, 2, 3, 'DORA', 'DORA', NULL, '1.0.0', 
            '2025-01-31', '2025-01-31', 'c0a78332d1ffb8448495194dfce5efe2', 1, 0
        )
        """,
    )
    new_conn.commit()

    changes = next(iter(compare_databases(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    print("Changes found:")
    for i, change in enumerate(changes):
        print(f"  {i}: old={dict(change.old) if change.old else None}")
        print(f"      new={dict(change.new) if change.new else None}")

    # Categorize changes
    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    print(f"Modified: {len(modified)}, Added: {len(added)}, Removed: {len(removed)}")

    # Should be identified as 1 modification (matched by RowGUID) instead of removal + addition
    assert len(removed) == 0, f"Expected 0 removals but got {len(removed)}"
    assert len(added) == 0, f"Expected 0 additions but got {len(added)}"

    # Should be 1 modification since the ModuleVID (PK) changed, even though other content is the same
    assert len(modified) == 1, f"Expected 1 modification but got {len(modified)}"
    
    # Verify the match is by RowGUID
    change = modified[0]
    assert change.old["RowGUID"] == change.new["RowGUID"] == "c0a78332d1ffb8448495194dfce5efe2"
    assert change.old["ModuleVID"] == 390
    assert change.new["ModuleVID"] == 391  # Different PK


def test_dora_module_case_with_actual_change() -> None:
    """Test same case but with an actual content change to verify modification detection."""
    old_conn = connect(":memory:")
    old_conn.execute(
        """
        CREATE TABLE modules (
            ModuleVID INTEGER PRIMARY KEY,
            RowGUID TEXT,
            Code TEXT,
            IsReported INTEGER
        )
        """,
    )
    old_conn.execute(
        "INSERT INTO modules VALUES (390, 'c0a78332d1ffb8448495194dfce5efe2', 'DORA', 1)",
    )
    old_conn.commit()

    new_conn = connect(":memory:")
    new_conn.execute(
        """
        CREATE TABLE modules (
            ModuleVID INTEGER PRIMARY KEY,
            RowGUID TEXT,
            Code TEXT,
            IsReported INTEGER
        )
        """,
    )
    new_conn.execute(
        "INSERT INTO modules VALUES (391, 'c0a78332d1ffb8448495194dfce5efe2', 'DORA_UPDATED', 0)",  # Content changed
    )
    new_conn.commit()

    changes = next(iter(compare_databases(old_conn, new_conn)))["rows"]["changes"]
    changes = list(changes)

    modified = [c for c in changes if c.old and c.new]
    added = [c for c in changes if c.new and not c.old]
    removed = [c for c in changes if c.old and not c.new]

    # Should identify as 1 modification (matched by RowGUID despite different PK)
    assert len(modified) == 1, f"Expected 1 modification but got {len(modified)}"
    assert len(added) == 0, f"Expected 0 additions but got {len(added)}"
    assert len(removed) == 0, f"Expected 0 removals but got {len(removed)}"

    # Verify the match details
    change = modified[0]
    assert (
        change.old["RowGUID"]
        == change.new["RowGUID"]
        == "c0a78332d1ffb8448495194dfce5efe2"
    )
    assert change.old["ModuleVID"] == 390
    assert change.new["ModuleVID"] == 391  # Different PK
    assert change.old["Code"] == "DORA"
    assert change.new["Code"] == "DORA_UPDATED"  # Content changed


if __name__ == "__main__":
    test_dora_module_case()
    test_dora_module_case_with_actual_change()
    print("✅ Real-world DORA tests passed!")
