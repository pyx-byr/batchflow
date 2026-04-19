"""Batch pipeline with checkpointing and retry support."""

import logging
from typing import Callable, Iterable, Any

from batchflow.checkpoint import Checkpoint
from batchflow.retry import RetryConfig, retry_call

logger = logging.getLogger(__name__)


class BatchPipeline:
    """Processes items in batches with optional checkpointing and retry."""

    def __init__(
        self,
        name: str,
        processor: Callable[[Any], Any],
        checkpoint_dir: str = ".checkpoints",
        retry_config: RetryConfig = None,
    ):
        self.name = name
        self.processor = processor
        self.checkpoint = Checkpoint(name=name, directory=checkpoint_dir)
        self.retry_config = retry_config or RetryConfig(max_attempts=1)

    def _process_item(self, item: Any) -> Any:
        """Process a single item, applying retry logic."""
        return retry_call(
            self.processor,
            args=(item,),
            config=self.retry_config,
        )

    def run(self, items: Iterable[Any], resume: bool = True) -> list:
        """Run the pipeline over all items."""
        state = self.checkpoint.load() if resume else {}
        results = state.get("results", [])
        completed = state.get("completed", 0)

        items = list(items)
        logger.info(
            "Pipeline '%s': %d items total, resuming from item %d.",
            self.name, len(items), completed
        )

        for idx, item in enumerate(items[completed:], start=completed):
            result = self._process_item(item)
            results.append(result)
            self.checkpoint.save({"results": results, "completed": idx + 1})
            logger.debug("Processed item %d/%d.", idx + 1, len(items))

        self.checkpoint.clear()
        return results
