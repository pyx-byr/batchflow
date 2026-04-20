"""SplitterConfig — split pipeline items into multiple sub-items.

Allows a single input item to be expanded into many items before
downstream processing, e.g. splitting a document into sentences or
a batch record into individual rows.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, List, Optional


@dataclass
class SplitterConfig:
    """Configuration for splitting pipeline items into sub-items.

    Attributes:
        name: Human-readable label for this splitter (useful for logging).
        split_fn: Callable that takes one item and returns an iterable of
                  sub-items.  Defaults to ``None`` (identity — no splitting).
        skip_empty: When *True* (default), sub-items that are ``None`` or
                    empty strings/lists/dicts are dropped silently.
        max_splits: Optional upper bound on the number of sub-items produced
                    from a single source item.  Excess sub-items are dropped.
    """

    name: str = "splitter"
    split_fn: Optional[Callable[[Any], Iterable[Any]]] = None
    skip_empty: bool = True
    max_splits: Optional[int] = None

    def __post_init__(self) -> None:
        if self.max_splits is not None and self.max_splits < 1:
            raise ValueError(
                f"max_splits must be a positive integer, got {self.max_splits}"
            )

    # ------------------------------------------------------------------
    # Fluent builder helpers
    # ------------------------------------------------------------------

    def set_fn(self, fn: Callable[[Any], Iterable[Any]]) -> "SplitterConfig":
        """Set the split function and return *self* for chaining."""
        self.split_fn = fn
        return self

    def set_max_splits(self, n: int) -> "SplitterConfig":
        """Set *max_splits* and return *self* for chaining."""
        if n < 1:
            raise ValueError(f"max_splits must be a positive integer, got {n}")
        self.max_splits = n
        return self

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    def split(self, item: Any) -> List[Any]:
        """Split *item* into a list of sub-items.

        If no ``split_fn`` is configured the original item is returned
        wrapped in a single-element list (identity behaviour).

        Args:
            item: The source item to split.

        Returns:
            A list of sub-items produced by ``split_fn`` (or ``[item]``).
        """
        if self.split_fn is None:
            return [item]

        results: List[Any] = []
        for sub in self.split_fn(item):
            if self.skip_empty and _is_empty(sub):
                continue
            results.append(sub)
            if self.max_splits is not None and len(results) >= self.max_splits:
                break

        return results


# ---------------------------------------------------------------------------
# Module-level helper
# ---------------------------------------------------------------------------

def _is_empty(value: Any) -> bool:
    """Return *True* if *value* is considered empty."""
    if value is None:
        return True
    if isinstance(value, (str, list, dict, tuple, set)) and len(value) == 0:
        return True
    return False


def apply_splitter(
    splitter: Optional[SplitterConfig],
    item: Any,
) -> List[Any]:
    """Convenience wrapper used by the pipeline.

    If *splitter* is ``None`` the item is returned as-is in a list.

    Args:
        splitter: A :class:`SplitterConfig` instance or ``None``.
        item: The item to (potentially) split.

    Returns:
        List of sub-items.
    """
    if splitter is None:
        return [item]
    return splitter.split(item)
