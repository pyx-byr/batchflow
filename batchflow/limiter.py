from dataclasses import dataclass, field
from typing import Optional


@dataclass
class LimiterConfig:
    """Limits the total number of items processed by a pipeline."""

    name: str = "limiter"
    max_items: Optional[int] = None
    _count: int = field(default=0, init=False, repr=False)

    def __post_init__(self):
        if self.max_items is not None and self.max_items <= 0:
            raise ValueError("max_items must be a positive integer")

    def set_max(self, max_items: int) -> "LimiterConfig":
        """Set the maximum number of items to process."""
        if max_items <= 0:
            raise ValueError("max_items must be a positive integer")
        self.max_items = max_items
        return self

    def is_limited(self) -> bool:
        """Return True if the limit has been reached."""
        if self.max_items is None:
            return False
        return self._count >= self.max_items

    def increment(self) -> None:
        """Record that one item has been processed."""
        self._count += 1

    def reset(self) -> None:
        """Reset the internal counter."""
        self._count = 0

    @property
    def count(self) -> int:
        return self._count


def apply_limiter(item, limiter: Optional[LimiterConfig]):
    """Check the limiter before processing an item.

    Returns the item if it should be processed, or raises StopIteration
    if the limit has been reached.
    """
    if limiter is None:
        return item
    if limiter.is_limited():
        raise StopIteration("Item limit reached")
    limiter.increment()
    return item
