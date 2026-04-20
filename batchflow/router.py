from dataclasses import dataclass, field
from typing import Callable, Dict, List, Any, Optional


@dataclass
class RouterConfig:
    """Routes items to different processing branches based on predicates."""
    name: str = "router"
    _routes: Dict[str, Callable[[Any], bool]] = field(default_factory=dict, init=False, repr=False)
    _default: Optional[str] = field(default=None, init=False, repr=False)

    def add_route(self, label: str, predicate: Callable[[Any], bool]) -> "RouterConfig":
        """Register a named route with a predicate function."""
        self._routes[label] = predicate
        return self

    def set_default(self, label: str) -> "RouterConfig":
        """Set the default route label when no predicate matches."""
        self._default = label
        return self

    def route(self, item: Any) -> Optional[str]:
        """Return the label of the first matching route, or the default."""
        for label, predicate in self._routes.items():
            try:
                if predicate(item):
                    return label
            except Exception:
                continue
        return self._default

    def route_labels(self) -> List[str]:
        """Return all registered route labels."""
        return list(self._routes.keys())


def apply_router(items: List[Any], router: RouterConfig) -> Dict[str, List[Any]]:
    """Partition items into buckets by route label."""
    buckets: Dict[str, List[Any]] = {}
    for item in items:
        label = router.route(item)
        if label is not None:
            buckets.setdefault(label, []).append(item)
    return buckets
