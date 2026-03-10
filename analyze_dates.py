"""Analyze date/datetime formats across all DPM database versions."""

import json
import re
import sqlite3
import tempfile
import zipfile
from collections import Counter, defaultdict
from io import BytesIO
from pathlib import Path

import requests

# All versions with converted SQLite databases (from versions.toml)
VERSIONS = {
    "3.2-sample": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-3.2-sample/dpm-3.2-sample-sqlite.zip",
    "3.5-sample": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-3.5-sample/dpm-3.5-sample-sqlite.zip",
    "4.0-draft": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-4.0-draft/dpm-4.0-draft-sqlite.zip",
    "4.0-release": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-4.0-release/dpm-4.0-release-sqlite.zip",
    "4.0-errata-4": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-4.0-errata-4/dpm-4.0-errata-4-sqlite.zip",
    "4.0-errata-5": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-4.0-errata-5/dpm-4.0-errata-5-sqlite.zip",
    "4.1-draft": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-4.1-draft/dpm-4.1-draft-sqlite.zip",
    "4.1-final": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-4.1-final/dpm-4.1-final-sqlite.zip",
    "4.1-errata-1": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-4.1-errata-1/dpm-4.1-errata-1-sqlite.zip",
    "4.2-draft": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-4.2-draft/dpm-4.2-draft-sqlite.zip",
    "4.2-release": "https://github.com/JimLundin/dpm-toolkit/releases/download/db-4.2-release/dpm-4.2-release-sqlite.zip",
}

# Date/datetime patterns to detect
DATE_PATTERNS = {
    "YYYY-MM-DD": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    "DD/MM/YYYY": re.compile(r"^\d{2}/\d{2}/\d{4}$"),
    "MM/DD/YYYY": re.compile(r"^\d{2}/\d{2}/\d{4}$"),  # ambiguous with above
    "YYYY/MM/DD": re.compile(r"^\d{4}/\d{2}/\d{2}$"),
    "DD-MM-YYYY": re.compile(r"^\d{2}-\d{2}-\d{4}$"),
}

DATETIME_PATTERNS = {
    "YYYY-MM-DD HH:MM:SS": re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
    "YYYY-MM-DDTHH:MM:SS": re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"),
    "YYYY-MM-DD HH:MM:SS.f": re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$"),
    "YYYY-MM-DDTHH:MM:SS.f": re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+$"),
    "DD/MM/YYYY HH:MM:SS": re.compile(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"),
    "YYYY-MM-DDTHH:MM:SS+TZ": re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$"),
    "YYYY-MM-DD HH:MM:SS+TZ": re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$"),
}

# Broad pattern to detect anything that looks date-like
BROAD_DATE_RE = re.compile(r"\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4}")


def download_db(version_id, url):
    """Download and extract SQLite database from ZIP."""
    print(f"  Downloading {version_id}...", flush=True)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/96.0.4664.93 Safari/537.36"
        ),
    }
    resp = requests.get(url, timeout=60, allow_redirects=True, headers=headers)
    resp.raise_for_status()

    zf = zipfile.ZipFile(BytesIO(resp.content))
    # Find the .sqlite file in the zip
    sqlite_files = [n for n in zf.namelist() if n.endswith(".sqlite")]
    if not sqlite_files:
        # Try any file
        sqlite_files = [n for n in zf.namelist() if not n.endswith("/")]

    if not sqlite_files:
        print(f"  WARNING: No files found in ZIP for {version_id}")
        return None

    # Extract to temp file
    tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
    tmp.write(zf.read(sqlite_files[0]))
    tmp.close()
    print(f"  Extracted: {sqlite_files[0]}", flush=True)
    return tmp.name


def classify_value(val):
    """Classify a string value into a date/datetime format."""
    if not isinstance(val, str):
        return None

    val = val.strip()
    if not val:
        return None

    # Check datetime patterns first (more specific)
    for fmt_name, pattern in DATETIME_PATTERNS.items():
        if pattern.match(val):
            return fmt_name

    # Check date patterns
    for fmt_name, pattern in DATE_PATTERNS.items():
        if pattern.match(val):
            # For DD/MM/YYYY vs MM/DD/YYYY ambiguity, just return the slash format
            if fmt_name in ("DD/MM/YYYY", "MM/DD/YYYY"):
                return "DD/MM/YYYY or MM/DD/YYYY"
            return fmt_name

    # Check if it's a broad date-like value we haven't categorized
    if BROAD_DATE_RE.search(val):
        return f"OTHER: {val[:30]}"

    return None


def analyze_database(db_path):
    """Analyze all columns in a SQLite database for date formats."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    results = {}  # table.column -> {format: count}
    sample_values = {}  # table.column -> [sample values]

    for table in tables:
        # Get column info
        cursor.execute(f"PRAGMA table_info([{table}])")
        columns = cursor.fetchall()

        for col in columns:
            col_name = col[1]
            col_type = col[2] or ""

            # Check all TEXT columns and also columns with date-related names/types
            is_date_type = any(
                kw in col_type.upper()
                for kw in ("DATE", "TIME", "TIMESTAMP")
            )
            is_date_name = any(
                kw in col_name.lower()
                for kw in ("date", "time", "timestamp")
            )
            is_text = "TEXT" in col_type.upper() or "VARCHAR" in col_type.upper() or col_type == ""

            # For efficiency, focus on columns likely to contain dates
            # But also scan all text columns for hidden dates
            full_col = f"{table}.{col_name}"

            if is_date_type or is_date_name:
                # Definitely check this column
                pass
            elif is_text:
                # Sample check - look at a few rows to see if they contain dates
                try:
                    cursor.execute(
                        f"SELECT [{col_name}] FROM [{table}] WHERE [{col_name}] IS NOT NULL LIMIT 5"
                    )
                    samples = [row[0] for row in cursor.fetchall()]
                    if not any(classify_value(str(s)) for s in samples):
                        continue
                except Exception:
                    continue
            else:
                # Numeric/blob columns - skip unless date-typed
                if not is_date_type:
                    continue

            # Full scan of this column
            try:
                cursor.execute(
                    f"SELECT [{col_name}], COUNT(*) FROM [{table}] WHERE [{col_name}] IS NOT NULL GROUP BY [{col_name}]"
                )
                rows = cursor.fetchall()
            except Exception as e:
                print(f"  Error reading {full_col}: {e}")
                continue

            format_counts = Counter()
            samples_for_col = []
            unclassified = []

            for val, count in rows:
                fmt = classify_value(str(val))
                if fmt:
                    format_counts[fmt] += count
                    if len(samples_for_col) < 5:
                        samples_for_col.append(str(val))
                else:
                    # Check if this is a date column with non-date values
                    if is_date_type or is_date_name:
                        if len(unclassified) < 3:
                            unclassified.append(str(val))

            if format_counts:
                results[full_col] = {
                    "type": col_type,
                    "formats": dict(format_counts),
                    "total_date_values": sum(format_counts.values()),
                }
                sample_values[full_col] = samples_for_col
                if unclassified:
                    results[full_col]["unclassified_samples"] = unclassified

    conn.close()
    return results, sample_values


def main():
    all_results = {}
    aggregate_formats = Counter()  # format -> total count across all DBs
    aggregate_by_column = defaultdict(Counter)  # column_name -> {format: count}

    for version_id, url in VERSIONS.items():
        print(f"\n{'='*60}")
        print(f"Analyzing: {version_id}")
        print(f"{'='*60}")

        db_path = download_db(version_id, url)
        if not db_path:
            continue

        try:
            results, samples = analyze_database(db_path)
            all_results[version_id] = {"columns": results, "samples": samples}

            if not results:
                print("  No date/datetime values found")
                continue

            print(f"\n  Found date/datetime values in {len(results)} columns:")
            for col, info in sorted(results.items()):
                print(f"\n    {col} (type: {info['type']}):")
                for fmt, count in sorted(info["formats"].items(), key=lambda x: -x[1]):
                    print(f"      {fmt}: {count} values")
                    aggregate_formats[fmt] += count
                    # Extract just the column name for aggregate
                    col_name = col.split(".")[-1]
                    aggregate_by_column[col_name][fmt] += count
                if "unclassified_samples" in info:
                    print(f"      Unclassified: {info['unclassified_samples']}")
                if col in samples:
                    print(f"      Samples: {samples[col][:3]}")
        finally:
            Path(db_path).unlink(missing_ok=True)

    # Print aggregate summary
    print(f"\n\n{'='*60}")
    print("AGGREGATE SUMMARY ACROSS ALL VERSIONS")
    print(f"{'='*60}")

    print("\n--- Overall format distribution ---")
    for fmt, count in sorted(aggregate_formats.items(), key=lambda x: -x[1]):
        print(f"  {fmt}: {count} total values")

    print(f"\n--- Formats by column name ---")
    for col_name, formats in sorted(aggregate_by_column.items()):
        print(f"\n  {col_name}:")
        for fmt, count in sorted(formats.items(), key=lambda x: -x[1]):
            print(f"    {fmt}: {count}")

    # Save detailed results to JSON
    output = {
        "per_version": {},
        "aggregate": {
            "format_distribution": dict(aggregate_formats),
            "by_column": {k: dict(v) for k, v in aggregate_by_column.items()},
        },
    }
    for vid, data in all_results.items():
        output["per_version"][vid] = {
            col: info for col, info in data["columns"].items()
        }

    output_path = Path("date_format_analysis.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nDetailed results saved to {output_path}")


if __name__ == "__main__":
    main()
