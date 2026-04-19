"""Batch splitting utilities for BatchFlow pipelines."""
from typing import Iterable, TypeVar, Iterator, List

T = TypeVar("T")


class BatchConfig:
    """Configuration for splitting an iterable into fixed-size batches."""

    def __init__(self, size: int = 10, drop_last: bool = False, label: str = "batch"):
        if size < 1:
            raise ValueError("Batch size must be >= 1")
        self.size = size
        self.drop_last = drop_last
        self.label = label

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"BatchConfig(size={self.size}, drop_last={self.drop_last}, "
            f"label={self.label!r})"
        )


def iter_batches(iterable: Iterable[T], config: BatchConfig) -> Iterator[List[T]]:
    """Yield successive batches from *iterable* according to *config*.

    Args:
        iterable: Any iterable of items.
        config: A :class:`BatchConfig` controlling batch size and behaviour.

    Yields:
        Lists of items whose length equals ``config.size`` (or less for the
        final batch when ``drop_last`` is *False*).
    """
    batch: List[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) == config.size:
            yield batch
            batch = []
    if batch and not config.drop_last:
        yield batch


def split_batches(iterable: Iterable[T], config: BatchConfig) -> List[List[T]]:
    """Return all batches as a list (eager evaluation)."""
    return list(iter_batches(iterable, config))
