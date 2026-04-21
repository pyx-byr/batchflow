from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class EnricherConfig:
    """Attaches additional fields to pipeline items via registered enrichment functions."""

    name: str = "enricher"
    _enrichers: List[Dict[str, Any]] = field(default_factory=list, init=False, repr=False)

    def add(self, key: str, fn: Callable[[Any], Any], *, default: Any = None) -> "EnricherConfig":
        """Register an enrichment function that adds *key* to each item."""
        if not callable(fn):
            raise TypeError(f"Enricher fn for key '{key}' must be callable")
        self._enrichers.append({"key": key, "fn": fn, "default": default})
        return self

    def enrich(self, item: Any) -> Any:
        """Apply all registered enrichers to *item* and return the enriched item."""
        if not isinstance(item, dict):
            raise TypeError("EnricherConfig.enrich expects a dict item")
        for spec in self._enrichers:
            try:
                item[spec["key"]] = spec["fn"](item)
            except Exception:
                item[spec["key"]] = spec["default"]
        return item

    @property
    def enricher_count(self) -> int:
        return len(self._enrichers)

    def reset(self) -> "EnricherConfig":
        """Remove all registered enrichers."""
        self._enrichers.clear()
        return self


def apply_enricher(item: Any, enricher: Optional[EnricherConfig]) -> Any:
    """Convenience wrapper used by the pipeline."""
    if enricher is None:
        return item
    return enricher.enrich(item)
