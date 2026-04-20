"""SplitterConfig: splits a single item into multiple sub-items for downstream processing."""

from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, List, Optional


@dataclass
class SplitterConfig:
    """Configuration for splitting pipeline items into sub-items.

    Useful when a single input item (e.g. a batch record, a file, a chunk)
    needs to be expanded into multiple independent items before further
    processing.

    Example usage::

        splitter = (
            SplitterConfig()
            .set_fn(lambda item: item.get("rows", [item]))
            .set_max_splits(100)
        )
    """

    name: str = "splitter"
    _fn: Optional[Callable[[Any], Iterator[Any]]] = field(default=None, repr=False, init=False)
    _max_splits: Optional[int] = field(default=None, repr=False, init=False)

    def __post_init__(self) -> None:
        if self._max_splits is not None and self._max_splits < 1:
            raise ValueError("max_splits must be a positive integer")

    def set_fn(self, fn: Callable[[Any], Iterator[Any]]) -> "SplitterConfig":
        """Set the function used to split an item into sub-items.

        The function must accept a single item and return an iterable of
        sub-items.
        """
        if not callable(fn):
            raise TypeError("fn must be callable")
        self._fn = fn
        return self

    def set_max_splits(self, max_splits: int) -> "SplitterConfig":
        """Limit the number of sub-items produced per input item."""
        if max_splits < 1:
            raise ValueError("max_splits must be a positive integer")
        self._max_splits = max_splits
        return self

    def split(self, item: Any) -> List[Any]:
        """Split *item* into a list of sub-items.

        If no split function has been configured the original item is returned
        wrapped in a single-element list (i.e. a no-op split).

        If *max_splits* is set, at most that many sub-items are returned.
        """
        if self._fn is None:
            return [item]

        results: List[Any] = []
        for sub in self._fn(item):
            results.append(sub)
            if self._max_splits is not None and len(results) >= self._max_splits:
                break

        return results

    @property
    def max_splits(self) -> Optional[int]:
        """Return the configured max_splits limit (or *None* if unlimited)."""
        return self._max_splits

    @property
    def fn(self) -> Optional[Callable[[Any], Iterator[Any]]]:
        """Return the configured split function (or *None* if not set)."""
        return self._fn


def apply_splitter(
    splitter: Optional[SplitterConfig], item: Any
) -> List[Any]:
    """Apply *splitter* to *item* and return the resulting sub-items.

    If *splitter* is ``None`` the item is returned as-is in a list.
    """
    if splitter is None:
        return [item]
    return splitter.split(item)
