from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class MapperConfig:
    """Configuration for applying named field-level mapping functions to items."""

    name: str = "mapper"
    _mappings: Dict[str, Callable[[Any], Any]] = field(default_factory=dict, init=False, repr=False)

    def add(self, key: str, fn: Callable[[Any], Any]) -> "MapperConfig":
        """Register a mapping function for a given field key."""
        if not callable(fn):
            raise TypeError(f"Mapping function for '{key}' must be callable")
        self._mappings[key] = fn
        return self

    def map(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Apply all registered mappings to a copy of the item dict."""
        if not isinstance(item, dict):
            raise TypeError("MapperConfig.map expects a dict item")
        result = dict(item)
        for key, fn in self._mappings.items():
            if key in result:
                result[key] = fn(result[key])
        return result

    @property
    def mapping_count(self) -> int:
        return len(self._mappings)

    def reset(self) -> None:
        """Remove all registered mappings."""
        self._mappings.clear()

    def keys(self) -> List[str]:
        return list(self._mappings.keys())


def apply_mapper(mapper: Optional[MapperConfig], item: Any) -> Any:
    """Apply mapper to item if mapper is configured, otherwise return item unchanged."""
    if mapper is None or mapper.mapping_count == 0:
        return item
    return mapper.map(item)
