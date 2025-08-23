"""Command line interface for DPM Toolkit."""

from datetime import date
from json import dumps
from pathlib import Path
from sqlite3 import OperationalError, connect
from typing import TYPE_CHECKING

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
from typer import Exit, Typer, echo

if TYPE_CHECKING:
    from collections.abc import Iterable

app = Typer(
    name="dpm-toolkit",
    help="DPM Toolkit CLI tool",
    no_args_is_help=True,
)

CWD = Path.cwd()

VERSIONS = get_versions()
VERSION_IDS = [v["id"] for v in VERSIONS]


def date_serializer(obj: object) -> str | None:
    """Convert date to ISO format."""
    if isinstance(obj, date):
        return obj.isoformat()
    return None


@app.command()
def versions(
    group: Group = Group.RELEASE,
    *,
    latest: bool = False,
    json: bool = False,
) -> None:
    """List available database versions."""
    version_group: Iterable[Version] = get_versions_by_type(VERSIONS, group)
    if latest:
        version = latest_version(version_group)
        if json:
            echo(dumps(version, default=date_serializer))
        else:
            echo("\n".join(f"{key}: {value}" for key, value in version.items()))
        return

    if json:
        echo(dumps(list(version_group), default=date_serializer))
    else:
        for version in version_group:
            for key, value in version.items():
                echo(f"{key}: {value}")
            echo("\n" + "-" * 40 + "\n")


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
        echo(f"Error: Invalid version '{version_id}'", err=True)
        raise Exit(1)

    version_id = version_obj["id"]
    echo(f"Downloading version {version_id} ({variant})")

    # Get source
    db_source = get_source(version_obj, variant)

    target_folder = output / version_id
    target_folder.mkdir(parents=True, exist_ok=True)

    echo(f"Downloading from: {db_source.get('url', 'unknown')}")

    archive = download_source(db_source)

    if extract:
        extract_archive(archive, target_folder)
    else:
        archive_path = target_folder / f"{version_id}.zip"
        archive_path.write_bytes(archive.getbuffer())

    echo(f"Downloaded version {version_id} to {target_folder}")


@app.command()
def migrate(source: Path, target: Path = CWD) -> None:
    """Migrate Access databases to SQLite."""
    try:
        from migrate import migrate_to_sqlite
    except ImportError as e:
        echo("Error: Migration requires Windows with ODBC drivers", err=True)
        raise Exit(1) from e

    echo(f"Migrating from: {source}")
    echo(f"Migrating to: {target}")

    migrate_to_sqlite(source, target)


@app.command()
def schema(source: Path, output: Path = CWD) -> None:
    """Generate SQLAlchemy schema from SQLite database."""
    try:
        from schema import generate_schema
    except ImportError as e:
        echo("Error: Schema generation requires additional dependencies", err=True)
        raise Exit(1) from e

    echo(f"Generating schema from: {source}")
    echo(f"Output to: {output}")

    generate_schema(source, output)


@app.command()
def compare(
    source: Path,
    target: Path,
    output: Path = CWD,
    *,
    html: bool = False,
) -> None:
    """Compare two SQLite databases."""
    try:
        from compare import compare_dbs, comparisons_to_html, comparisons_to_json
    except ImportError as e:
        echo(f"Error: Compare functionality not available: {e}", err=True)
        raise Exit(1) from e

    echo("Comparing databases:")
    echo(f"  Source: {source}")
    echo(f"  Target: {target}")

    # Perform comparison
    source_conn = connect(source)
    target_conn = connect(target)

    try:
        comparisons = compare_dbs(source_conn, target_conn)
    except OperationalError as e:
        echo(f"Error during comparison: {e}", err=True)
        raise Exit(1) from e
    finally:
        source_conn.close()
        target_conn.close()

    # Handle HTML output
    if html:
        output_path = output or Path("comparison_report.html")
        comparisons_to_html(comparisons).dump(str(output_path), encoding="utf-8")
        echo(f"HTML report saved to: {output_path}")
        return

    # Handle JSON output
    formatted_output = comparisons_to_json(comparisons)

    if output:
        output.write_text(formatted_output, encoding="utf-8")
        echo(f"Report saved to: {output}")
    else:
        print(formatted_output)


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
