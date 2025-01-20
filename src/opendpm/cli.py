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


def create_parser() -> argparse.ArgumentParser:
    """Create the command line argument parser."""
    parser = argparse.ArgumentParser(description="OpenDPM CLI tool")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Download command
    download_parser = subparsers.add_parser(
        "download", help="Download Access databases",
    )
    download_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to sources.toml config file",
    )
    download_parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to save downloaded databases",
    )

    # Convert command
    convert_parser = subparsers.add_parser(
        "convert", help="Convert Access databases to other formats",
    )
    convert_parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing Access databases",
    )
    convert_parser.add_argument(
        "--duckdb-path",
        type=Path,
        help="Path for DuckDB output file",
    )
    convert_parser.add_argument(
        "--sqlite-path",
        type=Path,
        help="Path for SQLite output file",
    )

    return parser


def main() -> None:
    """Entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "download":
        download.download_databases(args.config, args.output_dir)
    elif args.command == "convert":
        engines: list[str] = []
        if args.duckdb_path:
            engines.append(f"duckdb:///{args.duckdb_path}")
        if args.sqlite_path:
            engines.append(f"sqlite:///{args.sqlite_path}")

        if not engines:
            logger.error(
                "At least one output format "
                "(--duckdb-path or --sqlite-path) "
                "must be specified",
            )
            return

        convert.migrate_databases(args.input_dir, engines)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
