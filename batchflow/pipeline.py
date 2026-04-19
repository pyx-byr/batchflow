"""Batch processing pipeline with checkpointing, retry, and filtering support."""
from typing import Callable, Iterable, Any, Optional, List

from batchflow.checkpoint import Checkpoint
from batchflow.retry import RetryConfig, with_retry
from batchflow.progress import ProgressTracker
from batchflow.filter import FilterConfig


class BatchPipeline:
    """Processes items in batches with optional checkpointing, retry, and filtering."""

    def __init__(
        self,
        process_fn: Callable[[Any], Any],
        checkpoint: Optional[Checkpoint] = None,
        retry_config: Optional[RetryConfig] = None,
        filters: Optional[List[FilterConfig]] = None,
    ):
        self.process_fn = process_fn
        self.checkpoint = checkpoint
        self.retry_config = retry_config
        self.filters: List[FilterConfig] = filters or []
        self.progress = ProgressTracker()

    def _passes_filters(self, item: Any) -> bool:
        return all(f.should_process(item) for f in self.filters)

    def _process_item(self, item: Any) -> Any:
        fn = self.process_fn
        if self.retry_config:
            fn = with_retry(self.retry_config)(fn)
        return fn(item)

    def run(self, items: Iterable[Any]) -> List[Any]:
        """Run the pipeline over items, skipping already-processed ones."""
        results = []
        for item in items:
            key = str(item)

            if self.checkpoint and self.checkpoint.exists(key):
                self.progress.increment_skipped()
                results.append(self.checkpoint.load(key))
                continue

            if not self._passes_filters(item):
                self.progress.increment_skipped()
                continue

            try:
                result = self._process_item(item)
                if self.checkpoint:
                    self.checkpoint.save(key, result)
                self.progress.increment()
                results.append(result)
            except Exception:
                self.progress.increment_failed()
                raise

        return results
