"""Type registry for database migration transformations.

This module provides a clean, priority-based registry for defining type transformations
that are applied during database migration. All type rules are defined once and applied
consistently during schema reflection.
"""

from collections.abc import Callable
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, Enum, Inspector, Uuid
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.types import TypeEngine

type Matcher = Callable[[str], bool]


class TypeRegistry:
    """Priority-based registry for column type transformations."""

    def __init__(self) -> None:
        """Initialize a registry of columns and type matches."""
        # Store as (priority, matcher, sql_type) tuples, sorted by priority descending
        self._rules: list[tuple[int, Matcher, TypeEngine[Any]]] = []

    def exact(self, name: str, sql_type: TypeEngine[Any], priority: int = 100) -> None:
        """Register rule for exact column name match.

        Args:
            name: Exact column name to match
            sql_type: SQLAlchemy type to apply
            priority: Rule priority (higher = evaluated first), defaults to 100

        """
        self.register(lambda column_name: column_name == name, sql_type, priority)

    def suffix(
        self,
        suffix: str,
        sql_type: TypeEngine[Any],
        priority: int = 50,
    ) -> None:
        """Register rule for columns ending with suffix.

        Args:
            suffix: Suffix to match (case-insensitive)
            sql_type: SQLAlchemy type to apply
            priority: Rule priority (higher = evaluated first), defaults to 50

        """
        self.register(
            lambda column_name: column_name.lower().endswith(suffix.lower()),
            sql_type,
            priority,
        )

    def prefix(
        self,
        prefix: str,
        sql_type: TypeEngine[Any],
        priority: int = 50,
    ) -> None:
        """Register rule for columns starting with prefix.

        Args:
            prefix: Prefix to match (case-insensitive)
            sql_type: SQLAlchemy type to apply
            priority: Rule priority (higher = evaluated first), defaults to 50

        """
        self.register(
            lambda column_name: column_name.lower().startswith(prefix.lower()),
            sql_type,
            priority,
        )

    def register(
        self,
        matcher: Matcher,
        sql_type: TypeEngine[Any],
        priority: int = 25,
    ) -> None:
        """Register rule with custom matcher function.

        Args:
            matcher: Function that takes column name and returns bool
            sql_type: SQLAlchemy type to apply
            priority: Rule priority (higher = evaluated first), defaults to 25

        """
        self._rules.append((priority, matcher, sql_type))
        # Keep sorted by priority (highest first)

    def column_type(self, column_name: str) -> TypeEngine[Any] | None:
        """Get SQL type for column name - first match by priority wins.

        Args:
            column_name: Column name to look up

        Returns:
            SQLAlchemy type if match found, None otherwise

        """
        for _priority, matcher, sql_type in sorted(
            self._rules,
            key=lambda x: x[0],
            reverse=True,
        ):
            if matcher(column_name):
                return sql_type
        return None

    def list_rules(self) -> list[tuple[int, str, str]]:
        """List all registered rules for debugging/inspection.

        Returns:
            List of (priority, matcher_description, sql_type_name) tuples

        """
        rules: list[tuple[int, str, str]] = []
        for priority, matcher, sql_type in self._rules:
            # Try to get a description of the matcher function
            matcher_desc = getattr(matcher, "__name__", "custom_matcher")
            type_name = type(sql_type).__name__
            rules.append((priority, matcher_desc, type_name))
        return rules

    def genericize(
        self,
        _inspector: Inspector,
        _table_name: str,
        column: ReflectedColumn,
    ) -> None:
        """Genericize for SQLAlchemy compatibility only."""
        if new_type := self.column_type(column["name"]):
            column["type"] = new_type
        else:
            column["type"] = column["type"].as_generic()


def create_default_registry() -> TypeRegistry:
    """Create a type registry with default migration rules.

    This sets up the standard type transformations used in the migration process,
    matching the existing business logic but in a centralized, priority-based system.

    Returns:
        Configured TypeRegistry with default rules

    """
    registry = TypeRegistry()

    # Exact column name overrides (highest priority)
    registry.exact("ParentFirst", Boolean())
    registry.exact("UseIntervalArithmetics", Boolean())

    # Specific date/time columns (higher priority than general date pattern)
    registry.exact("StartDate", DateTime())
    registry.exact("EndDate", DateTime())

    # Boolean columns
    registry.prefix("is", Boolean())
    registry.prefix("has", Boolean())

    # suffix columns
    registry.suffix("guid", Uuid())
    registry.suffix("date", Date())

    # Enum candidate columns - use empty Enum as placeholder
    registry.suffix("type", Enum())
    registry.suffix("status", Enum())
    registry.suffix("sign", Enum())
    registry.suffix("optionality", Enum())
    registry.suffix("direction", Enum())
    registry.suffix("number", Enum())
    registry.suffix("endorsement", Enum())
    registry.suffix("source", Enum())
    registry.suffix("severity", Enum())
    registry.suffix("errorcode", Enum())

    return registry
