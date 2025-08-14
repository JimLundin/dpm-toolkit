"""Command line interface for DPM Toolkit."""

import json
from argparse import ArgumentParser, Namespace
from collections.abc import Sequence
from datetime import date
from enum import StrEnum, auto
from pathlib import Path
from sys import stdout

import yaml
from archive import (
    Source,
    Version,
    compare_version_urls,
    download_source,
    extract_archive,
    get_version,
    get_versions,
    get_versions_by_type,
    latest_version,
)

VERSIONS = get_versions()
VERSION_IDS = [v["id"] for v in VERSIONS]

RELEASE = latest_version(get_versions_by_type(VERSIONS, "release", "errata"))
LATEST = latest_version(VERSIONS)


class Format(StrEnum):
    """Output format options."""

    JSON = auto()
    TABLE = auto()
    YAML = auto()


class Verbosity(StrEnum):
    """Verbosity level options."""

    DEBUG = auto()
    INFO = auto()
    VERBOSE = auto()
    QUIET = auto()


def output_data(
    data: Version | Sequence[Version],
    format_type: Format = Format.TABLE,
    verbosity: Verbosity = Verbosity.INFO,
) -> None:
    """Output data in the specified format."""
    if verbosity == Verbosity.QUIET and format_type != Format.JSON:
        return

    if format_type == Format.JSON:
        json.dump(
            data,
            stdout,
            default=date_serializer,
            indent=2 if verbosity == Verbosity.VERBOSE else None,
        )
    elif format_type == Format.YAML:
        print(yaml.safe_dump(data, default_flow_style=False))

    elif format_type == Format.TABLE:
        if isinstance(data, list) and data:
            # Print as table for list of dicts
            if verbosity == Verbosity.VERBOSE:
                for item in data:
                    print("\n".join(f"{key}: {value}" for key, value in item.items()))
                    print("---")
            else:
                print("\n".join(item["id"] for item in data))
        elif isinstance(data, dict):
            print("\n".join(f"{key}: {value}" for key, value in data.items()))
        else:
            print(data)


def log_info(message: str, verbosity: Verbosity = Verbosity.INFO) -> None:
    """Print info message if not quiet."""
    if verbosity == Verbosity.QUIET:
        return
    if verbosity == Verbosity.INFO:
        print(message)
        return
    if verbosity == Verbosity.VERBOSE:
        print(f"[VERBOSE] {message}")
        return


def add_common_arguments(subparser: ArgumentParser) -> None:
    """Add standardized common arguments to a subparser."""
    # Format options
    format_group = subparser.add_mutually_exclusive_group()
    format_group.add_argument(
        "--format",
        choices=Format.__members__.values(),
        default="table",
        help="Output format (default: %(default)s)",
    )
    format_group.add_argument(
        "--json",
        dest="format",
        action="store_const",
        const="json",
        help="Output in JSON format",
    )
    format_group.add_argument(
        "--table",
        dest="format",
        action="store_const",
        const="table",
        help="Output in table format",
    )
    format_group.add_argument(
        "--yaml",
        dest="format",
        action="store_const",
        const="yaml",
        help="Output in YAML format",
    )

    # Verbosity options
    verbosity_group = subparser.add_mutually_exclusive_group()
    verbosity_group.add_argument(
        "--verbosity",
        choices=Verbosity.__members__.values(),
        default="info",
        help="Set verbosity level (default: %(default)s)",
    )
    verbosity_group.add_argument(
        "--debug",
        dest="verbosity",
        action="store_const",
        const="debug",
        help="Enable debug output",
    )
    verbosity_group.add_argument(
        "--info",
        dest="verbosity",
        action="store_const",
        const="info",
        help="Enable info output",
    )
    verbosity_group.add_argument(
        "--verbose",
        dest="verbosity",
        action="store_const",
        const="verbose",
        help="Enable verbose output",
    )
    verbosity_group.add_argument(
        "--quiet",
        "-q",
        dest="verbosity",
        action="store_const",
        const="quiet",
        help="Suppress non-essential output",
    )


def add_version_argument(subparser: ArgumentParser) -> None:
    """Add standardized version argument to a subparser."""
    subparser.add_argument(
        "--version",
        "-v",
        choices=["latest", "release", *VERSION_IDS],
        default="release",
        help="Version: latest, release, or the version ID (default: %(default)s)",
    )


def create_parser() -> ArgumentParser:
    """Create the command line argument parser."""
    parser = ArgumentParser(description="DPM Toolkit CLI tool")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    list_parser = subparsers.add_parser(
        "list",
        help="List available database versions",
        description="Display available versions with their release dates and types",
    )
    add_common_arguments(list_parser)
    add_version_argument(list_parser)
    list_parser.set_defaults(version=None)  # Allow showing all versions by default

    update_parser = subparsers.add_parser(
        "update",
        help="Find new download urls",
        description="Find new download urls and check for updates",
    )
    add_common_arguments(update_parser)

    download_parser = subparsers.add_parser(
        "download",
        help="Download databases",
        description="Download a specific version of the DPM database",
    )
    add_common_arguments(download_parser)
    add_version_argument(download_parser)
    download_parser.add_argument(
        "--target",
        type=Path,
        default=Path.cwd(),
        help="Directory to save downloaded database (default: %(default)s)",
    )
    download_parser.add_argument(
        "--type",
        choices=["original", "archive", "converted"],
        default="converted",
        help="Type of database to download (default: %(default)s)",
    )
    download_parser.add_argument(
        "--extract",
        action="store_true",
        default=True,
        help="Extract archive after download (default: %(default)s)",
    )
    download_parser.add_argument(
        "--no-extract",
        dest="extract",
        action="store_false",
        help="Do not extract archive after download",
    )
    download_parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing files",
    )

    migrate_parser = subparsers.add_parser(
        "migrate",
        help="Migrate Access databases to SQLite",
    )
    migrate_parser.add_argument(
        "--source",
        type=Path,
        required=True,
        help="Path of Access database to migrate (required)",
    )
    migrate_parser.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Path to save migrated SQLite database (required)",
    )
    migrate_parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing SQLite databases",
    )

    schema_parser = subparsers.add_parser(
        "schema",
        help="Generate SQLAlchemy schema from Access database",
    )
    schema_parser.add_argument(
        "--source",
        "-s",
        type=Path,
        required=True,
        help="Path of the SQLite to generate schema from (required)",
    )
    schema_parser.add_argument(
        "--target",
        "-t",
        type=Path,
        help="Path to save SQLAlchemy schema file to (default: %(default)s)",
    )

    return parser


def handle_version(args: Namespace) -> Version | None:
    """Handle the 'version' subcommand."""
    version = getattr(args, "version", None)
    if version is None:
        return None

    if version == "release":
        return RELEASE
    if version == "latest":
        return LATEST
    return get_version(VERSIONS, version)


def date_serializer(obj: object) -> str | None:
    """Convert date to ISO format."""
    if isinstance(obj, date):
        return obj.isoformat()

    return None


def handle_list_command(args: Namespace) -> None:
    """Handle the 'list' subcommand."""
    if version := handle_version(args):
        output_data(version, args.format, args.verbosity)
        return
    output_data(VERSIONS, args.format, args.verbosity)


def handle_update_command(args: Namespace) -> None:
    """Handle the 'update' subcommand."""
    try:
        from scrape import get_active_reporting_frameworks
    except ImportError:
        log_info("Error: Update requires the 'scrape' extra", args.verbosity)
        return

    log_info("Fetching active reporting frameworks...", args.verbosity)
    active_reporting_frameworks = get_active_reporting_frameworks()
    log_info("Comparing with known versions...", args.verbosity)
    new_reporting_frameworks = compare_version_urls(active_reporting_frameworks)
    output_data(new_reporting_frameworks, args.format, args.verbosity)


def handle_source(args: Namespace, version: Version) -> Source | None:
    """Handle the 'source' subcommand."""
    source_type = getattr(args, "type", "original")

    if source_type == "original":
        return version.get("original")
    if source_type == "archive":
        return version.get("archive")
    if source_type == "converted":
        return version.get("converted")

    log_info(f"Error: Unknown source type '{source_type}'.", args.verbosity)

    return None


def handle_download_command(args: Namespace) -> None:
    """Handle the 'download' subcommand."""
    version = handle_version(args)
    if not version:
        log_info("Error: Invalid or missing version argument.", args.verbosity)
        return

    version_id = version["id"]
    log_info(f"Downloading version {version_id} ({args.type})", args.verbosity)
    log_info(f"Version details: {version}", args.verbosity)

    source = handle_source(args, version)
    if not source:
        log_info("Error: Source not available for this version", args.verbosity)
        return

    target_folder = args.target / version_id

    # Check if target exists and handle overwrite
    if target_folder.exists() and not args.overwrite:
        log_info(
            f"Error: Target {target_folder} already exists. Use --overwrite.",
            args.verbosity,
        )
        return

    log_info(f"Downloading from: {source.get('url', 'unknown')}", args.verbosity)
    archive = download_source(source)

    if args.extract:
        extract_archive(archive, target_folder)
    else:
        # Write archive bytes to a file inside target_folder
        target_folder.mkdir(parents=True, exist_ok=True)
        archive_file = target_folder / source.get("filename", f"{version_id}.archive")
        archive_file.write_bytes(archive.getbuffer())

    log_info(f"Downloaded version {version_id} to {target_folder}", args.verbosity)


def handle_migrate_command(args: Namespace) -> None:
    """Handle the 'migrate' subcommand."""
    verbosity = getattr(args, "verbosity", Verbosity.INFO)

    try:
        from migrate import migrate_to_sqlite
    except ImportError:
        log_info("Error: Migration requires Windows with ODBC drivers", verbosity)
        return

    log_info(f"Migrating from: {args.source}", verbosity)
    log_info(f"Migrating to: {args.target}", verbosity)
    if args.source is None or args.target is None:
        log_info(
            "Error: Both --source and --target arguments are required for migration.",
            verbosity,
        )
        return
    migrate_to_sqlite(args.source, args.target)


def handle_schema_command(args: Namespace) -> None:
    """Handle the 'generate-schema' subcommand."""
    verbosity = getattr(args, "verbosity", Verbosity.INFO)

    try:
        from schema import generate_schema
    except ImportError:
        log_info(
            "Error: Schema generation requires additional dependencies",
            args.verbosity,
        )
        return

    log_info(f"Generating schema from: {args.source}", verbosity)
    log_info(f"Output to: {args.target}", verbosity)
    generate_schema(args.source, args.target)


def main() -> None:
    """Entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "list":
        handle_list_command(args)
    elif args.command == "update":
        handle_update_command(args)
    elif args.command == "download":
        handle_download_command(args)
    elif args.command == "migrate":
        handle_migrate_command(args)
    elif args.command == "schema":
        handle_schema_command(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
