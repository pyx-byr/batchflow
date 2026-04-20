from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional


@dataclass
class BufferConfig:
    """Accumulates items and flushes them in bulk when a threshold is reached."""

    name: str = "buffer"
    max_size: int = 100
    flush_on_full: bool = True
    _items: List[Any] = field(default_factory=list, init=False, repr=False)
    _flush_handlers: List[Callable[[List[Any]], None]] = field(
        default_factory=list, init=False, repr=False
    )

    def __post_init__(self) -> None:
        if self.max_size < 1:
            raise ValueError("max_size must be at least 1")

    def add(self, item: Any) -> bool:
        """Add an item to the buffer. Returns True if a flush was triggered."""
        self._items.append(item)
        if self.flush_on_full and len(self._items) >= self.max_size:
            self.flush()
            return True
        return False

    def flush(self) -> List[Any]:
        """Flush all buffered items to registered handlers and clear the buffer."""
        items = list(self._items)
        self._items.clear()
        for handler in self._flush_handlers:
            handler(items)
        return items

    def on_flush(self, handler: Callable[[List[Any]], None]) -> "BufferConfig":
        """Register a flush handler. Returns self for chaining."""
        self._flush_handlers.append(handler)
        return self

    @property
    def size(self) -> int:
        """Current number of buffered items."""
        return len(self._items)

    @property
    def is_full(self) -> bool:
        return len(self._items) >= self.max_size

    def peek(self) -> List[Any]:
        """Return a copy of current items without flushing."""
        return list(self._items)

    def clear(self) -> None:
        """Discard all buffered items without flushing handlers."""
        self._items.clear()


def apply_buffer(buffer: Optional[BufferConfig], item: Any) -> bool:
    """Apply buffering to an item. Returns True if a flush was triggered."""
    if buffer is None:
        return False
    return buffer.add(item)
