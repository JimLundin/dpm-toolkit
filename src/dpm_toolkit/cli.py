"""Command line interface for DPM Toolkit."""

import sys
from collections.abc import Iterable
from datetime import date
from json import dumps
from pathlib import Path
from sys import stdout
from typing import Any, Literal

from archive import (
    Group,
    SourceType,
    Version,
    download_source,
    get_source,
    get_version,
    get_versions,
    get_versions_by_type,
    latest_version,
)
from cyclopts import App
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

app = App(help="DPM Toolkit CLI tool")


type Format = Literal["table", "json", "html"]


def serializer(obj: Any) -> str | tuple[Any, ...] | None:  # noqa: ANN401
    """Convert date to ISO format and iterables to tuples."""
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, Iterable):
        return tuple(
            obj,  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
        )
    return None


console = Console()
err_console = Console(stderr=True)

# Constants
CWD = Path.cwd()
ACCESS_EXTENSIONS = {".mdb", ".accdb"}
SQLITE_EXTENSIONS = {".sqlite", ".db", ".sqlite3"}
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


def validate_database_location(database_location: Path, *, exists: bool = True) -> None:
    """Validate database location."""
    if exists != database_location.exists():
        print_error(
            f"Database file "
            f"{'does not exist' if exists else 'already exists'}: "
            f"{database_location}",
        )
        sys.exit(1)


def validate_database_extension(
    database_location: Path,
    file_extensions: Iterable[str],
) -> None:
    """Validate database file extension."""
    if database_location.suffix.lower() not in file_extensions:
        print_error(
            f"Database file has invalid extension: {', '.join(file_extensions)}",
        )
        sys.exit(1)


def validate_output_path(output: Path) -> None:
    """Validate output path is writable."""
    output_dir = output.parent
    if not output_dir.exists():
        print_error(f"Output directory does not exist: {output_dir}")
        sys.exit(1)
    if not output_dir.is_dir():
        print_error(f"Output path parent is not a directory: {output_dir}")
        sys.exit(1)
    # Test write permissions by checking if we can create a file
    try:
        output.touch()
        output.unlink()
    except (PermissionError, OSError) as e:
        print_error(f"Cannot write to output path: {output} ({e})")
        sys.exit(1)


def format_version_table(version: Version) -> None:
    """Format version information as a rich table."""
    table = Table(show_header=False)
    table.add_column("Key", style="bold blue")
    table.add_column("Value")
    for key, value in version.items():
        table.add_row(key, str(value))
    console.print(table)


def format_comparison_table(data: Iterable[dict[str, Any]]) -> None:
    """Format comparison summary as a rich table."""
    if not data:
        console.print("No differences found between databases.")
        return

    table = Table(title="Database Comparison Results")
    table.add_column("Table", style="bold cyan")
    table.add_column("Column Changes", style="bold yellow")
    table.add_column("Row Changes", style="bold yellow")

    for comparison in data:
        table.add_row(
            comparison.get("name", ""),
            str(comparison.get("columns", 0)),
            str(comparison.get("rows", 0)),
        )

    console.print(table)


@app.command
def versions(
    group: Group = "all",
    fmt: Format = "table",
    *,
    latest: bool = False,
) -> None:
    """List available database versions."""
    version_group = get_versions_by_type(VERSIONS, group)
    if latest:
        version = latest_version(version_group)
        if fmt == "json":
            console.print_json(dumps(version, default=serializer))
        elif fmt == "html":
            print_error("HTML format for versions is not yet implemented")
            sys.exit(1)
        elif fmt == "table":
            format_version_table(version)
        return

    if fmt == "json":
        console.print_json(dumps(version_group, default=serializer))
    elif fmt == "html":
        print_error("HTML format for versions is not yet implemented")
        sys.exit(1)
    elif fmt == "table":
        for version in version_group:
            format_version_table(version)


@app.command
def download(version_id: str, variant: SourceType = "archive") -> None:
    """Download databases."""
    version = get_version(VERSIONS, version_id)
    if not version:
        print_error(f"Invalid version '{version_id}'")
        sys.exit(1)

    try:
        database_source = get_source(version, variant)
    except ValueError:
        print_error("Failed to download from archive, downloading from source.")
        database_source = get_source(version, "original")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=err_console,
    ) as progress:
        task = progress.add_task(
            f"Downloading version {version_id} ({variant})",
            total=None,
        )
        print_info(f"Source URL: {database_source.get('url', 'unknown')}")
        archive = download_source(database_source)
        progress.update(task, description="Processing archive...")

    stdout.buffer.write(archive.getbuffer())
    print_success(f"Downloaded version {version_id} ({variant})")


@app.command
def migrate(access_location: Path, sqlite_location: Path) -> None:
    """Migrate Access database to SQLite."""
    try:
        from migrate import access, access_to_sqlite
    except ImportError:
        print_error("Migration requires [migrate] extra dependencies")
        sys.exit(1)

    validate_database_location(access_location, exists=True)
    validate_database_extension(access_location, ACCESS_EXTENSIONS)
    print_info(f"Access: {access_location}")

    validate_database_location(sqlite_location, exists=False)
    validate_database_extension(sqlite_location, SQLITE_EXTENSIONS)
    print_info(f"SQLite: {sqlite_location}")

    access_database = access(access_location)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=err_console,
    ) as progress:
        progress.add_task("Migrating database...", total=None)
        sqlite_database = access_to_sqlite(access_database)
        with sqlite_database as connection:
            connection.execute(f"VACUUM INTO '{sqlite_location}'")

    print_success("Migration completed successfully")


@app.command()
def schema(
    sqlite_location: Path,
    fmt: Literal["json", "html", "python"] = "python",
) -> None:
    """Generate database schema in multiple formats."""
    try:
        from schema import (
            read_only_sqlite,
            schema_to_html,
            schema_to_sqlalchemy,
            sqlite_to_schema,
        )
    except ImportError:
        print_error("Schema generation requires [schema] extra dependencies")
        sys.exit(1)

    validate_database_location(sqlite_location, exists=True)
    validate_database_extension(sqlite_location, SQLITE_EXTENSIONS)
    print_info(f"Source database: {sqlite_location}")
    print_info(f"Output format: {fmt}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=err_console,
    ) as progress:
        progress.add_task("Generating schema...", total=None)
        sqlite_database = read_only_sqlite(sqlite_location)
        schema_data = sqlite_to_schema(sqlite_database)

        if fmt == "python":
            sqlalchemy_code = schema_to_sqlalchemy(schema_data)
            stdout.write(sqlalchemy_code)
        elif fmt == "json":
            json_output = dumps(schema_data)
            stdout.write(json_output)
        elif fmt == "html":
            html_output = schema_to_html(schema_data)
            stdout.write(html_output)

    print_success("Schema generation completed successfully")


@app.command
def compare(old_location: Path, new_location: Path, fmt: Format = "table") -> None:
    """Compare two SQLite databases."""
    try:
        from compare import (
            compare_databases,
            comparisons_to_html,
            comparisons_to_summary,
        )
    except ImportError:
        print_error("Comparison requires [compare] extra dependencies")
        sys.exit(1)

    validate_database_location(old_location, exists=True)
    validate_database_extension(old_location, SQLITE_EXTENSIONS)
    print_info(f"Old database: {old_location}")

    validate_database_location(new_location, exists=True)
    validate_database_extension(new_location, SQLITE_EXTENSIONS)
    print_info(f"New database: {new_location}")

    print_info(f"Output format: {fmt}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=err_console,
    ) as progress:
        progress.add_task("Comparing databases...", total=None)
        comparisons = compare_databases(old_location, new_location)

    # Output to stdout in requested format (keep stdout clean for data)
    if fmt == "html":
        html_stream = comparisons_to_html(comparisons)
        for chunk in html_stream:
            stdout.write(chunk)

    if fmt == "json":
        stdout.write(dumps(comparisons, default=serializer))

    if fmt == "table":
        comparison_summary = comparisons_to_summary(comparisons)
        format_comparison_table(comparison_summary)


@app.command
def analyze(
    database: Path,
    fmt: Literal["json", "markdown"] = "json",
    *,
    output: Path | None = None,
    confidence: float = 0.7,
) -> None:
    """Analyze database for type refinement opportunities."""
    try:
        from analysis import (
            analyze_database,
            create_engine_for_database,
            generate_report,
            validate_engine,
        )
    except ImportError:
        print_error("Analysis requires [analysis] extra dependencies")
        sys.exit(1)

    from sqlalchemy.exc import SQLAlchemyError

    validate_database_location(database, exists=True)
    validate_database_extension(database, SQLITE_EXTENSIONS | ACCESS_EXTENSIONS)

    # Validate confidence parameter bounds
    if not 0.0 <= confidence <= 1.0:
        print_error(f"Confidence must be between 0.0 and 1.0, got {confidence}")
        sys.exit(1)

    # Validate output path before analysis
    if output:
        validate_output_path(output)

    print_info(f"Database: {database}")
    print_info(f"Output format: {fmt}")
    print_info(f"Confidence threshold: {confidence}")

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=err_console,
        ) as progress:
            task = progress.add_task("Analyzing database...", total=None)

            # Create engine and validate schema
            try:
                engine = create_engine_for_database(database)
                validate_engine(engine)
            except (SQLAlchemyError, ValueError) as e:
                print_error(f"Failed to connect to database: {e}")
                sys.exit(1)

            # Analyze database
            try:
                recommendations, patterns = analyze_database(
                    engine,
                    confidence_threshold=confidence,
                )
            except SQLAlchemyError as e:
                print_error(f"Analysis failed: {e}")
                sys.exit(1)

            progress.update(task, description="Generating report...")

            # Generate report in requested format
            output_text = generate_report(database.stem, recommendations, patterns, fmt)

        # Write to stdout or file
        if output:
            try:
                output.write_text(output_text)
                print_success(f"Analysis complete! Report written to {output}")
            except (PermissionError, OSError) as e:
                print_error(f"Failed to write output file: {e}")
                sys.exit(1)
        else:
            stdout.write(output_text)
            print_success("Analysis complete!")
    except KeyboardInterrupt:
        print_error("Analysis interrupted by user")
        sys.exit(1)


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
