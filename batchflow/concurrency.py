from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, Any, Optional


@dataclass
class ConcurrencyConfig:
    """Configuration for concurrent batch item processing."""
    max_workers: int = 1
    timeout: Optional[float] = None

    def __post_init__(self):
        if self.max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be a positive number")

    @property
    def is_concurrent(self) -> bool:
        return self.max_workers > 1


def apply_concurrently(
    fn: Callable[[Any], Any],
    items: Iterable[Any],
    config: ConcurrencyConfig,
) -> list[tuple[Any, Any, Optional[Exception]]]:
    """
    Apply fn to each item using the given ConcurrencyConfig.
    Returns list of (item, result, exception) tuples.
    result is None on error; exception is None on success.
    """
    results = []

    if not config.is_concurrent:
        for item in items:
            try:
                results.append((item, fn(item), None))
            except Exception as e:
                results.append((item, None, e))
        return results

    futures = {}
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        for item in items:
            future = executor.submit(fn, item)
            futures[future] = item

        for future in as_completed(futures, timeout=config.timeout):
            item = futures[future]
            try:
                results.append((item, future.result(), None))
            except Exception as e:
                results.append((item, None, e))

    return results
