from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class PartitionerConfig:
    """Splits items into named partitions based on a key function."""

    name: str = "partitioner"
    _key_fn: Optional[Callable[[Any], str]] = field(default=None, init=False, repr=False)
    _partitions: Dict[str, List[Any]] = field(default_factory=dict, init=False, repr=False)

    def set_key(self, fn: Callable[[Any], str]) -> "PartitionerConfig":
        """Set the function used to determine a partition key for each item."""
        if not callable(fn):
            raise TypeError("key function must be callable")
        self._key_fn = fn
        return self

    def partition(self, item: Any) -> str:
        """Assign an item to a partition and return the partition key."""
        if self._key_fn is None:
            raise RuntimeError("No key function set. Call set_key() first.")
        key = self._key_fn(item)
        if key not in self._partitions:
            self._partitions[key] = []
        self._partitions[key].append(item)
        return key

    def get(self, key: str) -> List[Any]:
        """Return all items in the named partition."""
        return self._partitions.get(key, [])

    def keys(self) -> List[str]:
        """Return all current partition keys."""
        return list(self._partitions.keys())

    def reset(self) -> None:
        """Clear all partitions."""
        self._partitions.clear()

    @property
    def partition_count(self) -> int:
        """Return the number of distinct partitions."""
        return len(self._partitions)

    @property
    def total_items(self) -> int:
        """Return the total number of items across all partitions."""
        return sum(len(v) for v in self._partitions.values())


def apply_partitioner(
    partitioner: PartitionerConfig, items: List[Any]
) -> Dict[str, List[Any]]:
    """Partition a list of items and return a dict of partition -> items."""
    partitioner.reset()
    for item in items:
        partitioner.partition(item)
    return {key: partitioner.get(key) for key in partitioner.keys()}
