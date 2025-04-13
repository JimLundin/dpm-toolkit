"""Command line interface for OpenDPM."""

from argparse import ArgumentParser, Namespace
from enum import StrEnum, auto
from pathlib import Path

from opendpm.convert import convert_access_to_sqlite
from opendpm.download import fetch_version
from opendpm.versions import (
    Formats,
    Version,
    Versions,
    get_drafts,
    get_releases,
    get_version,
    get_versions,
    latest_version,
    render_version,
    render_versions,
)


class Sources(StrEnum):
    """Sources of database files."""

    ACCESS = auto()
    SQLITE = auto()


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
        "--format",
        "-f",
        choices=Formats,
        default=Formats.SIMPLE,
        help="Output format (default: %(default)s)",
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
        "--source",
        type=str,
        choices=Sources,
        default=Sources.SQLITE,
        help="Source type (default: %(default)s)",
    )

    # Convert command
    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert Access databases to SQLite",
    )
    convert_parser.add_argument(
        "--target",
        type=Path,
        default=Path.cwd(),
        help="Directory to save SQLite databases (default: %(default)s)",
    )
    convert_parser.add_argument(
        "--source",
        type=Path,
        default=Path.cwd(),
        help="Directory containing Access databases (default: %(default)s)",
    )

    return parser


def handle_group(group: Groups) -> Versions | None:
    """Handle the 'group' subcommand."""
    if versions := GROUPS.get(group):
        return versions
    print(f"Invalid group: {group}")
    return None


def handle_version(version: Options) -> Version | None:
    """Handle the 'version' subcommand."""
    if latest := LATEST.get(version):
        return latest
    return get_version(VERSIONS, version)


def handle_list_command(args: Namespace) -> None:
    """Handle the 'list' subcommand."""
    if versions_to_show := handle_group(args.group):
        print(render_versions(versions_to_show, args.format))
        return
    if version_to_show := handle_version(args.version):
        print(render_version(version_to_show, args.format))
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
            fetch_version(version, args.target, args.source)
        else:
            print(f"Version {args.version} not found")
    elif args.command == "convert":
        convert_access_to_sqlite(args.source, args.target)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
