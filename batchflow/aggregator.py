from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class AggregatorConfig:
    """Collects and aggregates items using named aggregation functions."""

    name: str = "aggregator"
    _aggregators: Dict[str, Callable[[List[Any]], Any]] = field(default_factory=dict, init=False, repr=False)
    _buckets: Dict[str, List[Any]] = field(default_factory=dict, init=False, repr=False)

    def add(self, label: str, fn: Callable[[List[Any]], Any]) -> "AggregatorConfig":
        """Register an aggregation function under a label."""
        self._aggregators[label] = fn
        self._buckets.setdefault(label, [])
        return self

    def collect(self, label: str, item: Any) -> "AggregatorConfig":
        """Add an item to the bucket for the given label."""
        if label not in self._buckets:
            self._buckets[label] = []
        self._buckets[label].append(item)
        return self

    def result(self, label: str) -> Any:
        """Return the aggregated result for a label."""
        if label not in self._aggregators:
            raise KeyError(f"No aggregator registered for label '{label}'")
        return self._aggregators[label](self._buckets.get(label, []))

    def results(self) -> Dict[str, Any]:
        """Return aggregated results for all registered labels."""
        return {label: self.result(label) for label in self._aggregators}

    def reset(self, label: Optional[str] = None) -> "AggregatorConfig":
        """Clear collected items for one or all labels."""
        if label is not None:
            self._buckets[label] = []
        else:
            for key in self._buckets:
                self._buckets[key] = []
        return self

    @property
    def aggregator_count(self) -> int:
        return len(self._aggregators)


def apply_aggregator(
    config: Optional[AggregatorConfig],
    label: str,
    item: Any,
) -> Any:
    """Collect an item into the aggregator bucket if config is provided."""
    if config is None:
        return item
    config.collect(label, item)
    return item
