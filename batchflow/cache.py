from dataclasses import dataclass, field
from typing import Any, Optional, Callable
import hashlib
import json


@dataclass
class CacheConfig:
    name: str = "cache"
    max_size: int = 128
    _store: dict = field(default_factory=dict, init=False, repr=False)
    _hits: int = field(default=0, init=False, repr=False)
    _misses: int = field(default=0, init=False, repr=False)

    def _make_key(self, item: Any) -> str:
        try:
            raw = json.dumps(item, sort_keys=True, default=str)
        except Exception:
            raw = str(item)
        return hashlib.md5(raw.encode()).hexdigest()

    def get(self, item: Any) -> Optional[Any]:
        key = self._make_key(item)
        if key in self._store:
            self._hits += 1
            return self._store[key]
        self._misses += 1
        return None

    def set(self, item: Any, result: Any) -> None:
        if len(self._store) >= self.max_size:
            oldest = next(iter(self._store))
            del self._store[oldest]
        key = self._make_key(item)
        self._store[key] = result

    def has(self, item: Any) -> bool:
        return self._make_key(item) in self._store

    def clear(self) -> None:
        self._store.clear()
        self._hits = 0
        self._misses = 0

    @property
    def hits(self) -> int:
        return self._hits

    @property
    def misses(self) -> int:
        return self._misses

    @property
    def size(self) -> int:
        return len(self._store)


def apply_cache(item: Any, fn: Callable, cache: CacheConfig) -> Any:
    if cache.has(item):
        return cache.get(item)
    result = fn(item)
    cache.set(item, result)
    return result
