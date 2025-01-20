"""Functions for converting Access databases to DuckDB and SQLite formats."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import polars as pl
import pyodbc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def read_access_database(file_path: Path) -> dict[str, pl.DataFrame]:
    """Read all tables from an Access database file."""
    connection_string = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={file_path};"
    )

    with pyodbc.connect(connection_string) as connection:
        logger.info("Connected to Access database %s", file_path)

        # Get table names
        with connection.cursor() as cursor:
            access_tables = cursor.tables(tableType="TABLE")
            table_names = [table.table_name for table in access_tables]
        logger.info("Found tables: %s", table_names)

        # Read each table
        tables: dict[str, pl.DataFrame] = {}
        for table_name in table_names:
            try:
                query = f"SELECT * FROM [{table_name}]"
                table_df = pl.read_database(query, connection)  # type: ignore
                if not table_df.is_empty():
                    tables[table_name] = table_df
                else:
                    logger.warning("Table %s is empty", table_name)
            except Exception:
                logger.exception("Failed to read table %s", table_name)
                continue

        if not tables:
            logger.warning("No tables found in %s", file_path)

        return tables


def save_to_duckdb(data: dict[str, pl.DataFrame], output_file: Path) -> None:
    """Save all tables in a DuckDB file."""
    with duckdb.connect(output_file) as con:  # type: ignore
        for table_name, _df in data.items():  # noqa: PERF102
            try:
                con.sql(f"CREATE OR REPLACE TABLE '{table_name}' AS SELECT * FROM _df")
                logger.info("Saved table %s to DuckDB", table_name)
            except Exception:
                logger.exception("Failed to create table %s in DuckDB", table_name)


def save_to_sqlite(data: dict[str, pl.DataFrame], output_file: Path) -> None:
    """Save all tables in a SQLite file."""
    con_str = f"sqlite:///{output_file}"
    for table_name, df in data.items():
        try:
            df.write_database(table_name, con_str, engine="adbc")  # type: ignore
            logger.info("Saved table %s to SQLite", table_name)
        except Exception:
            logger.exception("Failed to create table %s in SQLite", table_name)


def merge_dataframes(dfs: list[pl.DataFrame]) -> pl.DataFrame:
    """Merge multiple dataframes with the same schema."""
    if not dfs:
        return pl.DataFrame()
    return pl.concat(dfs, how="diagonal")


def process_access_files(input_dir: Path, output_dir: Path) -> None:
    """Process all Access files in the input directory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Dictionary to store all tables from all files
    all_tables: dict[str, list[pl.DataFrame]] = {}

    for access_file in input_dir.glob("*.accdb"):
        try:
            # Read tables from Access database
            data = read_access_database(access_file)

            if data:
                # Save individual database files
                duckdb_file = output_dir / f"{access_file.stem}.duckdb"
                sqlite_file = output_dir / f"{access_file.stem}.sqlite"

                save_to_duckdb(data, duckdb_file)
                save_to_sqlite(data, sqlite_file)
                logger.info("Successfully processed %s", access_file)

                # Collect tables for the total database
                for table_name, df in data.items():
                    if table_name not in all_tables:
                        all_tables[table_name] = []
                    all_tables[table_name].append(df)

        except Exception:
            logger.exception("Failed to process %s", access_file)

    # Create total databases if we have collected any tables
    if all_tables:
        try:
            # Merge tables with the same name
            total_data = {
                table_name: merge_dataframes(dfs)
                for table_name, dfs in all_tables.items()
            }

            # Save total databases
            total_duckdb = output_dir / "total.duckdb"
            total_sqlite = output_dir / "total.sqlite"

            save_to_duckdb(total_data, total_duckdb)
            save_to_sqlite(total_data, total_sqlite)
            logger.info("Successfully created total databases")

        except Exception:
            logger.exception("Failed to create total databases")


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[3]
    input_dir = project_root / ".scratch" / "db_input"
    output_dir = project_root / ".scratch" / "db_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    process_access_files(input_dir, output_dir)
