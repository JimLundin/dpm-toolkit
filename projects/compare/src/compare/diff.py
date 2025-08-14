"""Generic diffing utilities for database comparisons."""

from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any


class ChangeType(StrEnum):
    """Enumeration for change types in diffs."""

    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()


type Value = str | dict[str, Any] | None


@dataclass
class Diff:
    """Represents a single difference between two items."""

    type: ChangeType
    path: str  # dot-separated path to the difference
    old_value: Value = None
    new_value: Value = None

    def _format_value(self, value: Value) -> str:
        """Format a value for display in diff output."""
        if value is None:
            return "None"
        if not isinstance(value, dict):
            return str(value)
            # Format dictionary as key=value pairs for readability
        pairs = [f"{k}={v}" for k, v in value.items()]
        return "{" + ", ".join(pairs) + "}"

    def __str__(self) -> str:
        """Return a human-readable string representation of the diff item."""
        if self.type == ChangeType.ADDED:
            return f"+ {self.path}: {self._format_value(self.new_value)}"
        if self.type == ChangeType.REMOVED:
            return f"- {self.path}: {self._format_value(self.old_value)}"
        if self.type == ChangeType.MODIFIED:
            old_formatted = self._format_value(self.old_value)
            new_formatted = self._format_value(self.new_value)
            return f"~ {self.path}: {old_formatted} -> {new_formatted}"
        return f"{self.type} {self.path}"


def diff_sets(old: set[str], new: set[str], path: str = "") -> list[Diff]:
    """Compare two sets and return differences."""
    diffs: list[Diff] = []

    diffs.extend(
        Diff(
            ChangeType.ADDED,
            f"{path}.{item}" if path else item,
            new_value=item,
        )
        for item in new - old
    )
    diffs.extend(
        Diff(
            ChangeType.REMOVED,
            f"{path}.{item}" if path else item,
            old_value=item,
        )
        for item in old - new
    )

    return diffs


def diff_dicts(
    old: dict[str, Any],
    new: dict[str, Any],
    path: str = "",
) -> list[Diff]:
    """Compare two dictionaries and return differences."""
    diffs: list[Diff] = []

    diffs.extend(
        Diff(
            ChangeType.ADDED,
            f"{path}.{key}" if path else key,
            new_value=new[key],
        )
        for key in new.keys() - old.keys()
    )
    diffs.extend(
        Diff(
            ChangeType.REMOVED,
            f"{path}.{key}" if path else key,
            old_value=old[key],
        )
        for key in old.keys() - new.keys()
    )

    # Modified keys
    for key in old.keys() & new.keys():
        full_path = f"{path}.{key}" if path else key
        if old[key] != new[key]:
            # Recursively diff if both are dicts
            if isinstance(old[key], dict) and isinstance(new[key], dict):
                diffs.extend(diff_dicts(old[key], new[key], full_path))
            else:
                diffs.append(
                    Diff(ChangeType.MODIFIED, full_path, old[key], new[key]),
                )

    return diffs


def diff_counts(old: dict[str, int], new: dict[str, int]) -> list[Diff]:
    """Compare count dictionaries (e.g., row counts)."""
    return diff_dicts(old, new)


def format_diffs(diffs: list[Diff], title: str = "Changes") -> list[str]:
    """Format diff items into human-readable strings."""
    if not diffs:
        return [f"{title}: None"]

    lines = [f"{title}:"]
    lines.extend(f"  • {diff}" for diff in sorted(diffs, key=lambda x: x.path))

    return lines
