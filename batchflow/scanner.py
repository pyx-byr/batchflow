from dataclasses import dataclass, field
from typing import Callable, Any, List, Optional


@dataclass
class ScannerConfig:
    """Scans items and accumulates a running state across the pipeline."""

    name: str = "scanner"
    _scanners: List[dict] = field(default_factory=list, init=False, repr=False)
    _state: dict = field(default_factory=dict, init=False, repr=False)

    def add(self, key: str, fn: Callable[[Any, Any], Any], initial: Any = None) -> "ScannerConfig":
        """Register a scanner with a key, fold function, and initial state."""
        if not callable(fn):
            raise TypeError(f"Scanner function for '{key}' must be callable.")
        self._scanners.append({"key": key, "fn": fn, "initial": initial})
        self._state[key] = initial
        return self

    def scan(self, item: Any) -> Any:
        """Apply all scanners to the item, updating running state. Returns item unchanged."""
        for scanner in self._scanners:
            key = scanner["key"]
            fn = scanner["fn"]
            self._state[key] = fn(self._state[key], item)
        return item

    def get_state(self, key: str, default: Any = None) -> Any:
        """Return current accumulated state for a given key."""
        return self._state.get(key, default)

    def reset(self) -> None:
        """Reset all scanner states to their initial values."""
        for scanner in self._scanners:
            self._state[scanner["key"]] = scanner["initial"]

    @property
    def scanner_count(self) -> int:
        return len(self._scanners)

    @property
    def state_snapshot(self) -> dict:
        return dict(self._state)


def apply_scanner(item: Any, scanner: Optional[ScannerConfig]) -> Any:
    """Apply scanner to item if configured; returns item unchanged."""
    if scanner is None:
        return item
    return scanner.scan(item)
