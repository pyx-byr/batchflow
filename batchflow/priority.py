from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple
import heapq


@dataclass
class PriorityConfig:
    """Configures priority-based ordering of items in a pipeline."""

    name: str = "priority"
    reverse: bool = False  # True = lower number = higher priority
    default_priority: int = 0
    _key_fn: Optional[Callable[[Any], int]] = field(default=None, repr=False)
    _heap: List[Tuple[int, int, Any]] = field(default_factory=list, repr=False)
    _counter: int = field(default=0, repr=False)

    def set_key(self, fn: Callable[[Any], int]) -> "PriorityConfig":
        """Set a function that extracts priority from an item."""
        self._key_fn = fn
        return self

    def push(self, item: Any) -> "PriorityConfig":
        """Push an item onto the priority queue."""
        priority = self._key_fn(item) if self._key_fn else self.default_priority
        if self.reverse:
            priority = -priority
        heapq.heappush(self._heap, (priority, self._counter, item))
        self._counter += 1
        return self

    def pop(self) -> Any:
        """Pop the highest-priority item from the queue."""
        if not self._heap:
            raise IndexError("Priority queue is empty")
        _, _, item = heapq.heappop(self._heap)
        return item

    def peek(self) -> Any:
        """Peek at the highest-priority item without removing it."""
        if not self._heap:
            raise IndexError("Priority queue is empty")
        return self._heap[0][2]

    def is_empty(self) -> bool:
        return len(self._heap) == 0

    def size(self) -> int:
        return len(self._heap)

    def reset(self) -> "PriorityConfig":
        """Clear the queue."""
        self._heap.clear()
        self._counter = 0
        return self

    def drain(self) -> List[Any]:
        """Return all items in priority order, emptying the queue."""
        result = []
        while not self.is_empty():
            result.append(self.pop())
        return result


def apply_priority(items: List[Any], config: PriorityConfig) -> List[Any]:
    """Push all items into the priority queue and drain them in order."""
    config.reset()
    for item in items:
        config.push(item)
    return config.drain()
