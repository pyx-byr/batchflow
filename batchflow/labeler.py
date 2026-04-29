from dataclasses import dataclass, field
from typing import Callable, Any, Optional


@dataclass
class LabelerConfig:
    """Assigns labels to items based on registered labeling functions."""

    name: str = "labeler"
    _labelers: list = field(default_factory=list, init=False, repr=False)

    def add(self, fn: Callable[[Any], Optional[str]], label: Optional[str] = None) -> "LabelerConfig":
        """Register a labeling function. Returns self for chaining."""
        if not callable(fn):
            raise TypeError(f"Labeler function must be callable, got {type(fn)}")
        self._labelers.append((fn, label))
        return self

    def label(self, item: Any) -> list[str]:
        """Apply all labelers to an item and return a list of matched labels."""
        results = []
        for fn, override_label in self._labelers:
            try:
                result = fn(item)
                if result is True and override_label is not None:
                    results.append(override_label)
                elif isinstance(result, str) and result:
                    results.append(result)
                elif result is not None and result is not False:
                    results.append(str(result))
            except Exception:
                pass
        return results

    def primary_label(self, item: Any) -> Optional[str]:
        """Return the first matched label, or None if no labeler matches."""
        labels = self.label(item)
        return labels[0] if labels else None

    def labeler_count(self) -> int:
        """Return the number of registered labelers."""
        return len(self._labelers)

    def reset(self) -> "LabelerConfig":
        """Remove all registered labelers."""
        self._labelers.clear()
        return self


def apply_labeler(item: Any, labeler: Optional[LabelerConfig]) -> list[str]:
    """Apply a LabelerConfig to an item, returning labels or an empty list."""
    if labeler is None:
        return []
    return labeler.label(item)
