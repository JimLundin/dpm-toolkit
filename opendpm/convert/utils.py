"""Utility functions for database conversion."""

from pathlib import Path


def print_path(path: Path, max_length: int = 80) -> str:
    """Print a path, truncating it if it's longer than max_length."""
    print_path = str(path)

    if len(print_path) < max_length:
        return print_path

    for i in range(len(path.parts)):
        print_path = f"{path.anchor}...\\{'\\'.join(path.parts[i:])}{path.suffix}"
        if len(print_path) < max_length:
            break

    return print_path
