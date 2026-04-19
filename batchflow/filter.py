"""Item filtering support for batch pipelines."""
from typing import Callable, Any, List, Optional


class FilterConfig:
    """Configuration for filtering items in a batch pipeline."""

    def __init__(
        self,
        predicate: Callable[[Any], bool],
        log_filtered: bool = False,
        label: Optional[str] = None,
    ):
        """
        Args:
            predicate: A callable that returns True if the item should be processed.
            log_filtered: Whether to log filtered-out items.
            label: Optional label for this filter (used in logging).
        """
        self.predicate = predicate
        self.log_filtered = log_filtered
        self.label = label or "filter"

    def should_process(self, item: Any) -> bool:
        """Return True if the item passes the filter."""
        try:
            return bool(self.predicate(item))
        except Exception as exc:
            raise ValueError(
                f"Filter '{self.label}' raised an error on item {item!r}: {exc}"
            ) from exc


def apply_filters(items: List[Any], filters: List[FilterConfig]) -> List[Any]:
    """Apply a list of FilterConfig objects to a list of items.

    Args:
        items: The input items to filter.
        filters: A list of FilterConfig instances.

    Returns:
        Items that pass all filters.
    """
    result = []
    for item in items:
        if all(f.should_process(item) for f in filters):
            result.append(item)
    return result
