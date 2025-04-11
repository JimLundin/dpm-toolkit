"""Command line interface for OpenDPM."""

from argparse import ArgumentParser
from pathlib import Path

from opendpm.convert import convert_access_to_sqlite
from opendpm.download import fetch_version
from opendpm.versions import get_releases, get_versions, latest_release, render_version


def get_config_path() -> Path:
    """Get the default config file path."""
    return Path(__file__).parent / "sources.toml"


def create_parser() -> ArgumentParser:
    """Create the command line argument parser."""
    parser = ArgumentParser(description="OpenDPM CLI tool")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Download command
    download_parser = subparsers.add_parser(
        "download",
        help="Download Access databases",
    )
    download_parser.add_argument(
        "--target",
        type=Path,
        default=Path.cwd(),
        help="Directory to save downloaded database (default: %(default)s)",
    )
    download_parser.add_argument(
        "--version",
        help="Specific version ID to download",
    )
    download_parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify downloaded files using hash",
    )

    # List command
    list_parser = subparsers.add_parser(
        "list",
        help="List available database versions",
    )
    list_parser.add_argument(
        "--all",
        action="store_true",
        help="Show all versions (including betas)",
    )

    # Convert command
    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert Access databases to SQLite",
    )
    convert_parser.add_argument(
        "target",
        nargs="?",
        type=Path,
        default=Path.cwd(),
        help="Directory to save converted databases",
    )
    convert_parser.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=Path.cwd(),
        help="Directory containing Access databases",
    )

    return parser


def main() -> None:
    """Entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()
    versions = get_versions()
    releases = get_releases(versions)

    if args.command == "list":
        if args.all:
            print("\n".join(f"{render_version(v)}" for v in versions))  # noqa: T201
        else:
            print("\n".join(f"{render_version(v)}" for v in releases))  # noqa: T201

    elif args.command == "download":
        if args.version:
            fetch_version(args.version, args.target)
        else:
            release = latest_release(releases)
            fetch_version(release, args.target)

    elif args.command == "convert":
        convert_access_to_sqlite(args.source, args.target)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
