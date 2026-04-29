from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class ReducerConfig:
    """Combines multiple items into a single accumulated result using reduce functions."""

    name: str = "reducer"
    _reducers: Dict[str, Dict] = field(default_factory=dict, init=False, repr=False)

    def add(
        self,
        label: str,
        fn: Callable[[Any, Any], Any],
        initial: Any = None,
    ) -> "ReducerConfig":
        """Register a named reduce function with an optional initial value."""
        if not callable(fn):
            raise TypeError(f"Reducer '{label}' must be callable")
        self._reducers[label] = {"fn": fn, "initial": initial, "acc": initial}
        return self

    def reduce(self, label: str, item: Any) -> Any:
        """Apply the named reducer to the given item, updating its accumulator."""
        if label not in self._reducers:
            raise KeyError(f"No reducer registered under label '{label}'")
        entry = self._reducers[label]
        if entry["acc"] is None:
            entry["acc"] = item
        else:
            entry["acc"] = entry["fn"](entry["acc"], item)
        return entry["acc"]

    def result(self, label: str) -> Any:
        """Return the current accumulated result for a reducer."""
        if label not in self._reducers:
            raise KeyError(f"No reducer registered under label '{label}'")
        return self._reducers[label]["acc"]

    def results(self) -> Dict[str, Any]:
        """Return a dict of all reducer labels to their current accumulated values."""
        return {label: entry["acc"] for label, entry in self._reducers.items()}

    def reset(self, label: Optional[str] = None) -> None:
        """Reset one or all reducers back to their initial values."""
        targets = [label] if label else list(self._reducers.keys())
        for lbl in targets:
            if lbl not in self._reducers:
                raise KeyError(f"No reducer registered under label '{lbl}'")
            self._reducers[lbl]["acc"] = self._reducers[lbl]["initial"]

    def reducer_count(self) -> int:
        return len(self._reducers)


def apply_reducer(reducer: Optional[ReducerConfig], label: str, item: Any) -> Any:
    """Apply a reducer by label if one is configured; otherwise return the item unchanged."""
    if reducer is None:
        return item
    return reducer.reduce(label, item)
