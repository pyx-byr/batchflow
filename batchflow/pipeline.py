from typing import Any, Callable, Iterable, Iterator, List, Optional
from .checkpoint import Checkpoint


class BatchPipeline:
    """A resumable batch data processing pipeline with checkpointing support."""

    def __init__(
        self,
        pipeline_id: str,
        steps: List[Callable[[Any], Any]],
        checkpoint_dir: str = ".batchflow_checkpoints",
    ):
        self.pipeline_id = pipeline_id
        self.steps = steps
        self.checkpoint = Checkpoint(checkpoint_dir)

    def _process_item(self, item: Any) -> Any:
        for step in self.steps:
            item = step(item)
        return item

    def run(
        self,
        data: Iterable[Any],
        batch_size: int = 100,
        resume: bool = True,
    ) -> Iterator[List[Any]]:
        """Process data in batches, yielding results per batch.

        If resume=True and a checkpoint exists, processing skips already-completed batches.
        """
        start_batch = 0
        if resume and self.checkpoint.exists(self.pipeline_id):
            state = self.checkpoint.load(self.pipeline_id)
            start_batch = state["batch_index"] + 1
            print(f"[{self.pipeline_id}] Resuming from batch {start_batch}")

        batch: List[Any] = []
        batch_index = 0

        for item in data:
            batch.append(item)
            if len(batch) == batch_size:
                if batch_index >= start_batch:
                    results = [self._process_item(x) for x in batch]
                    self.checkpoint.save(self.pipeline_id, batch_index)
                    yield results
                batch = []
                batch_index += 1

        if batch and batch_index >= start_batch:
            results = [self._process_item(x) for x in batch]
            self.checkpoint.save(self.pipeline_id, batch_index)
            yield results

        self.checkpoint.clear(self.pipeline_id)
        print(f"[{self.pipeline_id}] Pipeline complete. Checkpoint cleared.")
