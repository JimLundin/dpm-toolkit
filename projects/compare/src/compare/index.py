"""Hierarchical indexing for multi-key matching."""

from collections.abc import Callable, Iterator
from sqlite3 import Row

type ValueType = str | int | float | None
type Key = tuple[ValueType, ...]
type IndexKey = tuple[str, Key]
type IndexKeys = list[IndexKey]
type Indexer = Callable[[Row], IndexKeys]


class HierarchicalIndex:
    """Index for hierarchical row matching with automatic key generation."""

    def __init__(self, indexer: Indexer) -> None:
        """Initialize the index with an indexer function."""
        self._indexer = indexer
        self._key_to_group_id: dict[IndexKey, int] = {}
        self._groups: dict[int, tuple[Row, tuple[IndexKey, ...]]] = {}
        self._next_group_id = 0

    def add(self, row: Row) -> None:
        """Add a row with semantic keys."""
        keys = tuple(self._indexer(row))

        group_id = self._next_group_id
        self._next_group_id += 1

        self._groups[group_id] = (row, keys)

        for key in keys:
            self._key_to_group_id[key] = group_id

    def pop(self, row: Row) -> Row | None:
        """Pop first available match for the given row, trying keys in order."""
        for key in self._indexer(row):
            if matched_row := self._pop_by_key(key):
                return matched_row
        return None

    def _pop_by_key(self, key: IndexKey) -> Row | None:
        """Pop value by specific key, removing from all keys."""
        group_id = self._key_to_group_id.get(key)
        if group_id is None:
            return None

        value, keys = self._groups.pop(group_id)

        # Remove all keys in this group
        for k in keys:
            self._key_to_group_id.pop(k, None)

        return value

    def __iter__(self) -> Iterator[Row]:
        """Iterate over all remaining unmatched rows."""
        for row, _keys in self._groups.values():
            yield row
