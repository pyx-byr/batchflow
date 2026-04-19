from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, List, Optional

from batchflow.checkpoint import Checkpoint
from batchflow.filter import FilterConfig
from batchflow.retry import RetryConfig, with_retry
from batchflow.transform import TransformConfig
from batchflow.sink import SinkConfig
from batchflow.logging import PipelineLogger
from batchflow.context import PipelineContext


@dataclass
class BatchPipeline:
    items: Iterable[Any]
    filters: List[FilterConfig] = field(default_factory=list)
    transforms: Optional[TransformConfig] = None
    sink: Optional[SinkConfig] = None
    checkpoint: Optional[Checkpoint] = None
    retry: Optional[RetryConfig] = None
    logger: Optional[PipelineLogger] = None
    context: Optional[PipelineContext] = None

    def _passes_filters(self, item: Any) -> bool:
        for f in self.filters:
            if not f.should_process(item):
                return False
        return True

    def _process_item(self, item: Any) -> Any:
        if self.transforms:
            item = self.transforms.apply(item)
        if self.sink:
            self.sink.emit(item)
        return item

    def run(self) -> List[Any]:
        if self.context:
            self.context.start()

        seen = set()
        if self.checkpoint:
            seen = self.checkpoint.load()

        results = []

        process_fn = self._process_item
        if self.retry:
            process_fn = with_retry(self.retry)(self._process_item)

        for item in self.items:
            item_id = str(item)
            if item_id in seen:
                if self.logger:
                    self.logger.info(f"Skipping already processed item: {item}")
                continue
            if not self._passes_filters(item):
                continue
            try:
                result = process_fn(item)
                results.append(result)
                seen.add(item_id)
                if self.checkpoint:
                    self.checkpoint.save(seen)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed to process item {item}: {e}")
                raise

        if self.context:
            self.context.stop()

        return results
