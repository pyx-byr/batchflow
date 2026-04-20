from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Set


@dataclass
class DedupeConfig:
    """Configuration for deduplication of pipeline items."""

    name: str = "dedupe"
    key_fn: Optional[Callable[[Any], Any]] = None
    _seen: Set[Any] = field(default_factory=set, init=False, repr=False)

    def is_duplicate(self, item: Any) -> bool:
        """Return True if the item has been seen before."""
        key = self.key_fn(item) if self.key_fn else item
        if key in self._seen:
            return True
        self._seen.add(key)
        return False

    def reset(self) -> "DedupeConfig":
        """Clear the seen set."""
        self._seen.clear()
        return self

    @property
    def seen_count(self) -> int:
        return len(self._seen)


def apply_dedupe(item: Any, dedupe: Optional[DedupeConfig]) -> bool:
    """Return True if item should be skipped (is a duplicate)."""
    if dedupe is None:
        return False
    return dedupe.is_duplicate(item)
