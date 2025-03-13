"""Command line interface for OpenDPM."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from opendpm import convert, download

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def get_default_config() -> Path:
    """Get the default config file path."""
    return Path(__file__).parent / "config" / "sources.toml"


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
        "output_dir",
        type=Path,
        help="Directory to save downloaded databases",
    )
    download_parser.add_argument(
        "--config",
        type=Path,
        default=get_default_config(),
        help="Path to sources.toml config file (default: %(default)s)",
    )

    # Convert command
    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert Access databases to DuckDB or SQLite",
    )
    convert_parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory containing Access databases",
    )
    convert_parser.add_argument(
        "output_dir",
        type=Path,
        help="Directory to save converted databases",
    )
    convert_parser.add_argument(
        "--format",
        type=str,
        default="sqlite",
        choices=["duckdb", "sqlite"],
        help="Target database format (default: sqlite)",
    )

    # Config path command
    subparsers.add_parser(
        "config-path",
        help="Print the path to the default config file",
    )

    return parser


def main() -> None:
    """Entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "download":
        download.download_databases(args.config, args.output_dir)
    elif args.command == "convert":
        convert.migrate_database(args.input_dir, args.output_dir, args.format)
    elif args.command == "config-path":
        # Print just the path without any logging output
        print(get_default_config())  # noqa: T201
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
