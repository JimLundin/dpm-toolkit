"""Command line interface for OpenDPM."""

from argparse import ArgumentParser, Namespace
from datetime import date
from enum import StrEnum, auto
from json import dumps
from pathlib import Path

from opendpm.convert import convert_access_to_sqlite
from opendpm.download import Types, fetch_version
from opendpm.versions import (
    Version,
    Versions,
    get_drafts,
    get_releases,
    get_version,
    get_versions,
    latest_version,
)


class Groups(StrEnum):
    """Groups of versions."""

    RELEASES = auto()
    DRAFTS = auto()
    ALL = auto()


class Options(StrEnum):
    """Options for the command line interface."""

    RELEASE = auto()
    DRAFT = auto()
    LATEST = auto()


VERSIONS = get_versions()
VERSION_IDS = [v["id"] for v in VERSIONS]
VERSION_CHOICES = [*Options, *VERSION_IDS]

LATEST: dict[Options, Version] = {
    Options.RELEASE: latest_version(get_releases(VERSIONS)),
    Options.DRAFT: latest_version(get_drafts(VERSIONS)),
    Options.LATEST: latest_version(VERSIONS),
}

GROUPS: dict[Groups, Versions] = {
    Groups.RELEASES: get_releases(VERSIONS),
    Groups.DRAFTS: get_drafts(VERSIONS),
    Groups.ALL: VERSIONS,
}


def create_parser() -> ArgumentParser:
    """Create the command line argument parser."""
    parser = ArgumentParser(description="OpenDPM CLI tool")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # List command
    list_parser = subparsers.add_parser(
        "list",
        help="List available database versions",
        description="Display available versions with their release dates and types",
    )

    version_groups = list_parser.add_mutually_exclusive_group()
    version_groups.add_argument(
        "--version",
        "-v",
        type=str,
        choices=VERSION_CHOICES,
        help="Version to display",
    )
    version_groups.add_argument(
        "--group",
        "-g",
        type=str,
        choices=Groups,
        default=Groups.ALL,
        help="Show only releases, drafts, or all versions (default: %(default)s)",
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format",
    )

    # Download command
    download_parser = subparsers.add_parser(
        "download",
        help="Download databases",
        description="Download a specific version of the DPM database",
    )
    download_parser.add_argument(
        "--version",
        type=str,
        choices=VERSION_CHOICES,
        default=Options.LATEST,
        help="Version to download (default: %(default)s)",
    )
    download_parser.add_argument(
        "--target",
        type=Path,
        default=Path.cwd(),
        help="Directory to save downloaded database (default: %(default)s)",
    )
    download_parser.add_argument(
        "--type",
        type=str,
        choices=Types,
        default=Types.SQLITE,
        help="Source type (default: %(default)s)",
    )

    # Convert command
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


def handle_version(version_id: Options) -> Version | None:
    """Handle the 'version' subcommand."""
    if latest := LATEST.get(version_id):
        return latest
    return get_version(VERSIONS, version_id)


def handle_group(group: Groups) -> Versions | None:
    """Handle the 'group' subcommand."""
    if versions := GROUPS.get(group):
        return versions
    print(f"Invalid group: {group}")
    return None


def date_serializer(obj: object) -> str | None:
    """Convert date to ISO format."""
    if isinstance(obj, date):
        return obj.isoformat()

    return None


def handle_list_command(args: Namespace) -> None:
    """Handle the 'list' subcommand."""
    if version_to_show := handle_version(args.version):
        if args.json:
            print(dumps(version_to_show, default=date_serializer))
        else:
            print(
                "\n".join(f"{key}: {value}" for key, value in version_to_show.items()),
            )
        return
    if versions_to_show := handle_group(args.group):
        if args.json:
            print(dumps(versions_to_show, default=date_serializer))
        else:
            print("\n".join(v["id"] for v in versions_to_show))
        return

    print("No versions available")


def main() -> None:
    """Entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "list":
        handle_list_command(args)
    elif args.command == "download":
        if version := handle_version(args.version):
            fetch_version(version, args.target, args.type)
        else:
            print(f"Version {args.version} not found")
    elif args.command == "convert":
        convert_access_to_sqlite(args.source, args.target)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
