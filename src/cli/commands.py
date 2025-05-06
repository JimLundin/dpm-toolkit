"""Command line interface for OpenDPM."""

from argparse import ArgumentParser, Namespace
from datetime import date
from json import dump
from pathlib import Path
from sys import stdout

from archive import (
    Source,
    Version,
    get_version,
    get_versions,
    get_versions_by_type,
    latest_version,
)
from downloader import download_source, extract_archive

VERSIONS = get_versions()
VERSION_IDS = [v["id"] for v in VERSIONS]

RELEASE = latest_version(get_versions_by_type(VERSIONS, "release", "errata"))
LATEST = latest_version(VERSIONS)


def create_parser() -> ArgumentParser:
    """Create the command line argument parser."""
    parser = ArgumentParser(description="OpenDPM CLI tool")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    list_parser = subparsers.add_parser(
        "list",
        help="List available database versions",
        description="Display available versions with their release dates and types",
    )

    version_groups = list_parser.add_mutually_exclusive_group()
    version_groups.add_argument(
        "--release",
        action="store_true",
        help="Show the latest release version",
    )
    version_groups.add_argument(
        "--latest",
        action="store_true",
        help="Show the latest version",
    )
    version_groups.add_argument(
        "--version",
        "-v",
        type=str,
        choices=VERSION_IDS,
        help="Version to display",
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format",
    )

    scrape_parser = subparsers.add_parser(
        "scrape",
        help="Find new download urls",
        description="Find new download urls",
    )
    scrape_parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format",
    )

    download_parser = subparsers.add_parser(
        "download",
        help="Download databases",
        description="Download a specific version of the DPM database",
    )
    version_groups = download_parser.add_mutually_exclusive_group()
    version_groups.add_argument(
        "--release",
        action="store_true",
        help="Show the latest release version",
    )
    version_groups.add_argument(
        "--latest",
        action="store_true",
        help="Show the latest version",
    )
    version_groups.add_argument(
        "--version",
        "-v",
        type=str,
        choices=VERSION_IDS,
        help="Version to download",
    )
    download_parser.add_argument(
        "--target",
        type=Path,
        default=Path.cwd(),
        help="Directory to save downloaded database (default: %(default)s)",
    )
    download_type = download_parser.add_mutually_exclusive_group()
    download_type.add_argument(
        "--original",
        action="store_true",
        help="Download original Access database",
    )
    download_type.add_argument(
        "--archive",
        action="store_true",
        help="Download archive Access database",
    )
    download_type.add_argument(
        "--converted",
        action="store_true",
        help="Download converted SQLite database, this is the default",
    )

    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert Access databases to SQLite",
    )
    convert_parser.add_argument(
        "--source",
        type=Path,
        default=Path.cwd(),
        help="Directory containing Access databases (default: %(default)s)",
    )
    convert_parser.add_argument(
        "--target",
        type=Path,
        default=Path.cwd(),
        help="Directory to save SQLite databases (default: %(default)s)",
    )

    return parser


def handle_version(args: Namespace) -> Version | None:
    """Handle the 'version' subcommand."""
    if args.release:
        return RELEASE
    if args.latest:
        return LATEST
    if args.version:
        return get_version(VERSIONS, args.version)
    return None


def date_serializer(obj: object) -> str | None:
    """Convert date to ISO format."""
    if isinstance(obj, date):
        return obj.isoformat()

    return None


def handle_list_command(args: Namespace) -> None:
    """Handle the 'list' subcommand."""
    if version := handle_version(args):
        if args.json:
            dump(version, stdout, default=date_serializer)
        else:
            print("\n".join(f"{key}: {value}" for key, value in version.items()))
        return
    if args.json:
        dump(VERSIONS, stdout, default=date_serializer)
    else:
        print("\n".join(VERSION_IDS))


def handle_update_command(args: Namespace) -> None:
    """Handle the 'update' subcommand."""
    try:
        from crawler import get_new_reporting_frameworks
    except ImportError:
        print("Please install the 'scrape' extra: pip install opendpm[scrape]")
        return

    new_reporting_frameworks = get_new_reporting_frameworks()
    if args.json:
        dump(new_reporting_frameworks, stdout)
        return
    print(get_new_reporting_frameworks())


def handle_source(args: Namespace, version: Version) -> Source | None:
    """Handle the 'source' subcommand."""
    if args.original:
        return version["original"]

    if args.archive:
        if archive := version.get("archive"):
            return archive
        return None

    if args.converted:
        if converted := version.get("converted"):
            return converted
        return None

    print("No source specified, using EBA source")
    return version["original"]


def handle_download_command(args: Namespace) -> None:
    """Handle the 'download' subcommand."""
    version = handle_version(args)
    if not version:
        print("No version specified, using latest release version")
        version = RELEASE

    version_id = version["id"]
    print(f"Downloading version {version_id}")
    source = handle_source(args, version)
    if not source:
        print("Source not available")
        return

    archive = download_source(source["url"], source.get("checksum"))
    target_folder = args.target / version_id
    extract_archive(archive, target_folder)
    print(f"Downloaded version {version_id} to {target_folder}")


def main() -> None:
    """Entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "list":
        handle_list_command(args)
    elif args.command == "scrape":
        handle_update_command(args)
    elif args.command == "download":
        handle_download_command(args)
    elif args.command == "convert":
        try:
            from convert import convert_access_to_sqlite
        except ImportError:
            print("Please install the 'convert' extra: pip install opendpm[convert]")
            return

        convert_access_to_sqlite(args.source, args.target)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
