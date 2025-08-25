"""Command line interface for DPM Toolkit."""

from collections.abc import Iterable
from datetime import date
from enum import StrEnum, auto
from json import dumps
from pathlib import Path
from sqlite3 import OperationalError, connect
from typing import Any

from archive import (
    Group,
    SourceType,
    Version,
    download_source,
    extract_archive,
    get_source,
    get_version,
    get_versions,
    get_versions_by_type,
    latest_version,
)
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from typer import Exit, Typer

app = Typer(name="dpm-toolkit", help="DPM Toolkit CLI tool", no_args_is_help=True)


class OutputFormats(StrEnum):
    """Output format types."""

    TABLE = auto()
    JSON = auto()
    HTML = auto()


def serializer[T](obj: date | Iterable[T]) -> str | tuple[T, ...] | None:
    """Convert date to ISO format."""
    if isinstance(obj, Iterable):
        return tuple(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    return None


console = Console()
err_console = Console(stderr=True)

# Constants
CWD = Path.cwd()
VERSIONS = get_versions()


def print_error(message: str) -> None:
    """Print error message to stderr."""
    err_console.print(f"[bold red]Error:[/] {message}")


def print_success(message: str) -> None:
    """Print success message to stderr."""
    err_console.print(f"[bold green]✓[/] {message}")


def print_info(message: str) -> None:
    """Print info message to stderr."""
    err_console.print(f"[bold blue]i[/] {message}")


def format_version_table(version: Version) -> None:
    """Format version information as a rich table."""
    table = Table(show_header=False)
    table.add_column("Key", style="bold blue")
    table.add_column("Value")
    for key, value in version.items():
        table.add_row(key, str(value))
    console.print(table)


def format_comparison_table(
    json_data: Iterable[dict[str, Any]],
    length: int = 100,
) -> None:
    """Format comparison results as a rich table."""
    if not json_data:
        console.print("No differences found between databases.")
        return

    table = Table(title="Database Comparison Results")
    table.add_column("Table", style="bold cyan")
    table.add_column("Change Type", style="bold yellow")
    table.add_column("Details", style="dim")

    for comparison in json_data:
        details = str(comparison.get("details", ""))
        truncated_details = (
            f"{details[:length]}..." if len(details) > length else details
        )
        table.add_row(
            comparison.get("name", ""),
            comparison.get("change_type", ""),
            truncated_details,
        )

    console.print(table)


@app.command()
def versions(
    group: Group = Group.ALL,
    output_format: OutputFormats = OutputFormats.TABLE,
    *,
    latest: bool = False,
) -> None:
    """List available database versions."""
    version_group = get_versions_by_type(VERSIONS, group)
    if latest:
        version = latest_version(version_group)
        if output_format == OutputFormats.JSON:
            console.print_json(dumps(version, default=serializer))
        elif output_format == OutputFormats.HTML:
            print_error("HTML format for versions is not yet implemented")
            raise Exit(1)
        elif output_format == OutputFormats.TABLE:
            format_version_table(version)
        return

    if output_format == OutputFormats.JSON:
        console.print_json(dumps(tuple(version_group), default=serializer))
    elif output_format == OutputFormats.HTML:
        print_error("HTML format for versions is not yet implemented")
        raise Exit(1)
    elif output_format == OutputFormats.TABLE:
        for version in version_group:
            format_version_table(version)


@app.command()
def download(
    version_id: str,
    output: Path = CWD,
    variant: SourceType = SourceType.ORIGINAL,
    *,
    extract: bool = True,
) -> None:
    """Download databases."""
    version_obj = get_version(VERSIONS, version_id)
    if not version_obj:
        print_error(f"Invalid version '{version_id}'")
        raise Exit(1)

    version_id = version_obj["id"]

    # Get source
    db_source = get_source(version_obj, variant)
    target_folder = output / version_id
    target_folder.mkdir(parents=True, exist_ok=True)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=err_console,
    ) as progress:
        task = progress.add_task(
            f"Downloading version {version_id} ({variant})",
            total=None,
        )
        print_info(f"Source URL: {db_source.get('url', 'unknown')}")

        archive = download_source(db_source)

        progress.update(task, description="Processing archive...")

        if extract:
            extract_archive(archive, target_folder)
        else:
            archive_path = target_folder / f"{version_id}.zip"
            archive_path.write_bytes(archive.getbuffer())

    print_success(f"Downloaded version {version_id} to {target_folder}")


@app.command()
def migrate(source_location: Path, target_location: Path) -> None:
    """Migrate Access databases to SQLite."""
    try:
        from migrate import access_engine, access_to_sqlite
    except ImportError as e:
        print_error("Migration requires [migrate] extra dependencies")
        raise Exit(1) from e

    if not source_location.exists():
        print_error(f"Source database file does not exist: {source_location}")
        raise Exit(1)
    if target_location.exists():
        print_error(f"Target database file already exists: {target_location}")
        raise Exit(1)

    if source_location.suffix.lower() not in {".mdb", ".accdb"}:
        print_error("Source file must have an Access extension: .mdb, .accdb")
        raise Exit(1)

    if target_location.suffix.lower() not in {".sqlite", ".db", ".sqlite3"}:
        print_error("Target file must have a SQLite extension: .sqlite, .db, .sqlite3")
        raise Exit(1)

    print_info(f"Source: {source_location}")
    print_info(f"Output: {target_location}")

    access_database = access_engine(source_location)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=err_console,
    ) as progress:
        progress.add_task("Migrating database...", total=None)
        sqlite_database = access_to_sqlite(access_database)
        with sqlite_database as connection:
            connection.execute(f"VACUUM INTO '{target_location}'")

    print_success("Migration completed successfully")


@app.command()
def schema(sqlite_location: Path) -> None:
    """Generate SQLAlchemy schema from SQLite database."""
    try:
        from schema import sqlite_read_only, sqlite_to_sqlalchemy_schema
    except ImportError as e:
        print_error("Schema generation requires [schema] extra dependencies")
        raise Exit(1) from e

    if not sqlite_location.exists():
        print_error(f"Source database file does not exist: {sqlite_location}")
        raise Exit(1)

    print_info(f"Source database: {sqlite_location}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=err_console,
    ) as progress:
        progress.add_task("Generating schema...", total=None)
        sqlite_database = sqlite_read_only(sqlite_location)
        sqlalchemy_schema = sqlite_to_sqlalchemy_schema(sqlite_database)
        console.print(sqlalchemy_schema)

    print_success("Schema generation completed successfully")


@app.command()
def compare(
    old_location: Path,
    new_location: Path,
    output_format: OutputFormats = OutputFormats.TABLE,
) -> None:
    """Compare two SQLite databases."""
    try:
        from compare import compare_databases, comparisons_to_html
    except ImportError as e:
        print_error("Comparison requires [compare] extra dependencies")
        raise Exit(1) from e

    print_info(f"Old database: {old_location}")
    print_info(f"New database: {new_location}")
    print_info(f"Output format: {output_format}")

    # Check if files exist
    if not old_location.exists():
        print_error(f"Old database file does not exist: {old_location}")
        raise Exit(1)
    if not new_location.exists():
        print_error(f"New database file does not exist: {new_location}")
        raise Exit(1)

    # Perform comparison
    try:
        old_database = connect(old_location)
        new_database = connect(new_location)
    except OperationalError as e:
        print_error(f"Failed to open database files: {e}")
        raise Exit(1) from e

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=err_console,
        ) as progress:
            progress.add_task("Comparing databases...", total=None)
            comparisons = compare_databases(old_database, new_database)

        # Output to stdout in requested format (keep stdout clean for data)
        if output_format == OutputFormats.HTML:
            html_stream = comparisons_to_html(comparisons)
            for chunk in html_stream:
                console.print(chunk, end="")
            return

        if output_format == OutputFormats.JSON:
            console.print_json(dumps(comparisons, default=serializer))
            return

        if output_format == OutputFormats.TABLE:
            format_comparison_table(comparisons)
            return

    except OperationalError as e:
        print_error(f"Database comparison failed: {e}")
        raise Exit(1) from e
    finally:
        old_database.close()
        new_database.close()


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
