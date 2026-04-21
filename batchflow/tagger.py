from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class TaggerConfig:
    """Assigns tags to items based on registered tagging functions."""

    name: str = "tagger"
    _taggers: Dict[str, Callable[[Any], bool]] = field(default_factory=dict, init=False, repr=False)

    def add(self, tag: str, fn: Callable[[Any], bool]) -> "TaggerConfig":
        """Register a tagging function for the given tag label."""
        if not callable(fn):
            raise TypeError(f"Tagger for '{tag}' must be callable.")
        self._taggers[tag] = fn
        return self

    def tag(self, item: Any) -> List[str]:
        """Return a list of tags that apply to the given item."""
        return [tag for tag, fn in self._taggers.items() if fn(item)]

    def has_tag(self, item: Any, tag: str) -> bool:
        """Return True if the item matches the given tag."""
        fn = self._taggers.get(tag)
        return bool(fn and fn(item))

    def tags(self) -> List[str]:
        """Return all registered tag labels."""
        return list(self._taggers.keys())

    def clear(self) -> "TaggerConfig":
        """Remove all registered taggers."""
        self._taggers.clear()
        return self


def apply_tagger(item: Any, tagger: Optional[TaggerConfig]) -> List[str]:
    """Apply a TaggerConfig to an item and return matched tags, or [] if no tagger."""
    if tagger is None:
        return []
    return tagger.tag(item)
