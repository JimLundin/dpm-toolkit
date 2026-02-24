"""Table allowlist for DPM Lite.

This module defines which tables from the full DPM 2.0 database are included
in the lightweight dpmlite package.  Tables are grouped by domain for clarity.

The canonical table names must match ``__tablename__`` values in the committed
``dpm2/models.py``.  The build pipeline validates this allowlist against the
full database before filtering.

To update the allowlist:
  1. Inspect the committed ``projects/dpm2/src/dpm2/models.py`` for available tables.
  2. Add or remove table names from the appropriate category set below.
  3. Run the dpmlite build to verify the filtered database and models are valid.
"""

# ---------------------------------------------------------------------------
# Report structure — templates, modules, and how reports are organised
# ---------------------------------------------------------------------------
# Populate from committed dpm2 models.py
REPORT_TABLES: set[str] = set()

# ---------------------------------------------------------------------------
# Table layout — the row/column structure of each reporting table
# ---------------------------------------------------------------------------
# Populate from committed dpm2 models.py
TABLE_STRUCTURE_TABLES: set[str] = set()

# ---------------------------------------------------------------------------
# Validation rules — the business rules applied to reported data
# ---------------------------------------------------------------------------
# Populate from committed dpm2 models.py
VALIDATION_TABLES: set[str] = set()

# ---------------------------------------------------------------------------
# Shared reference tables — needed by the above but not a domain on their own
# ---------------------------------------------------------------------------
# Populate from committed dpm2 models.py
# Foreign-key targets that multiple domains depend on
SHARED_TABLES: set[str] = set()

# ---------------------------------------------------------------------------
# Combined allowlist — the union used by the filtering pipeline
# ---------------------------------------------------------------------------
INCLUDED_TABLES: frozenset[str] = frozenset(
    REPORT_TABLES | TABLE_STRUCTURE_TABLES | VALIDATION_TABLES | SHARED_TABLES,
)
