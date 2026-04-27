from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional


@dataclass
class WindowConfig:
    """Sliding or tumbling window over a stream of items."""

    name: str = "window"
    size: int = 10
    step: int = 1
    _buffer: List[Any] = field(default_factory=list, init=False, repr=False)
    _on_window: Optional[Callable[[List[Any]], Any]] = field(default=None, init=False, repr=False)

    def __post_init__(self):
        if self.size <= 0:
            raise ValueError("Window size must be a positive integer.")
        if self.step <= 0:
            raise ValueError("Window step must be a positive integer.")

    def on_window(self, fn: Callable[[List[Any]], Any]) -> "WindowConfig":
        """Register a callback invoked with each complete window."""
        self._on_window = fn
        return self

    def add(self, item: Any) -> List[List[Any]]:
        """Add an item to the internal buffer and return any completed windows."""
        self._buffer.append(item)
        windows = []
        while len(self._buffer) >= self.size:
            window = list(self._buffer[: self.size])
            windows.append(window)
            if self._on_window is not None:
                self._on_window(window)
            self._buffer = self._buffer[self.step :]
        return windows

    def flush(self) -> Optional[List[Any]]:
        """Return remaining items as a partial window and clear the buffer."""
        if self._buffer:
            partial = list(self._buffer)
            self._buffer = []
            if self._on_window is not None:
                self._on_window(partial)
            return partial
        return None

    def reset(self) -> None:
        """Clear the internal buffer."""
        self._buffer = []

    @property
    def buffer_size(self) -> int:
        return len(self._buffer)


def apply_window(items, window: WindowConfig) -> List[List[Any]]:
    """Feed all items through the window and collect completed windows."""
    all_windows = []
    for item in items:
        all_windows.extend(window.add(item))
    partial = window.flush()
    if partial:
        all_windows.append(partial)
    return all_windows
