"""Command line interface for OpenDPM."""

from __future__ import annotations

import argparse
from pathlib import Path

from opendpm import convert, download


def get_config_path() -> Path:
    """Get the default config file path."""
    return Path(__file__).parent / "sources.toml"


def create_parser() -> argparse.ArgumentParser:
    """Create the command line argument parser."""
    parser = argparse.ArgumentParser(description="OpenDPM CLI tool")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Download command
    download_parser = subparsers.add_parser(
        "download",
        help="Download Access databases",
    )
    download_parser.add_argument(
        "target",
        nargs="?",
        type=Path,
        default=Path.cwd(),
        help="Directory to save downloaded databases",
    )
    download_parser.add_argument(
        "--config-path",
        type=Path,
        default=get_config_path(),
        help="Path to sources.toml config file (default: %(default)s)",
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
    convert_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing database",
    )

    # Config path command
    subparsers.add_parser(
        "config",
        help="Print the path to the default config file",
    )

    return parser


def main() -> None:
    """Entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "download":
        download.fetch_databases(args.config_path, args.target)
    elif args.command == "convert":
        convert.convert_access_to_sqlite(
            args.source,
            args.target,
            overwrite=args.overwrite,
        )
    elif args.command == "config":
        # Print just the path without any logging output
        print(get_config_path())  # noqa: T201
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
