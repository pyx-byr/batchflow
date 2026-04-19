from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional

from batchflow.checkpoint import Checkpoint
from batchflow.filter import FilterConfig, apply_filters
from batchflow.transform import TransformConfig, apply_transforms
from batchflow.sink import SinkConfig
from batchflow.retry import RetryConfig, with_retry
from batchflow.logging import PipelineLogger
from batchflow.context import PipelineContext
from batchflow.schema import SchemaConfig, apply_schema


@dataclass
class BatchPipeline:
    items: Iterable[Any]
    checkpoint: Optional[Checkpoint] = None
    filters: List[FilterConfig] = field(default_factory=list)
    transforms: List[TransformConfig] = field(default_factory=list)
    sinks: List[SinkConfig] = field(default_factory=list)
    retry: Optional[RetryConfig] = None
    logger: Optional[PipelineLogger] = None
    context: Optional[PipelineContext] = None
    schema: Optional[SchemaConfig] = None

    def _passes_filters(self, item: Any) -> bool:
        return all(apply_filters(item, [f]) for f in self.filters)

    def _passes_schema(self, item: Any) -> bool:
        if self.schema is None:
            return True
        if not isinstance(item, dict):
            return True
        errors = apply_schema(item, self.schema)
        if errors and self.logger:
            for e in errors:
                self.logger.warning(f"Schema error: {e}")
        return len(errors) == 0

    def _process_item(self, item: Any) -> Optional[Any]:
        for transform in self.transforms:
            item = apply_transforms(item, [transform])
            if item is None:
                return None
        for sink in self.sinks:
            sink.emit(item)
        return item

    def run(self) -> List[Any]:
        if self.context:
            self.context.start()

        seen = set()
        if self.checkpoint:
            seen = self.checkpoint.load()

        results = []
        for item in self.items:
            key = str(item)
            if key in seen:
                continue
            if not self._passes_filters(item):
                continue
            if not self._passes_schema(item):
                continue
            try:
                if self.retry:
                    result = with_retry(lambda i=item: self._process_item(i), self.retry)
                else:
                    result = self._process_item(item)
                if result is not None:
                    results.append(result)
                    seen.add(key)
                    if self.checkpoint:
                        self.checkpoint.save(seen)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed to process item {item}: {e}")
                continue

        if self.context:
            self.context.stop()

        return results
