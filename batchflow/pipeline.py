from typing import Any, Dict, Iterable, List, Optional
from batchflow.checkpoint import Checkpoint
from batchflow.filter import FilterConfig
from batchflow.schema import SchemaConfig
from batchflow.transform import TransformConfig
from batchflow.sink import SinkConfig
from batchflow.logging import PipelineLogger
from batchflow.context import PipelineContext
from batchflow.hook import HookConfig
from batchflow.metrics import MetricsCollector


class BatchPipeline:
    def __init__(
        self,
        name: str = "pipeline",
        checkpoint_dir: str = "/tmp/batchflow",
        filters: Optional[FilterConfig] = None,
        schema: Optional[SchemaConfig] = None,
        transform: Optional[TransformConfig] = None,
        sink: Optional[SinkConfig] = None,
        logger: Optional[PipelineLogger] = None,
        context: Optional[PipelineContext] = None,
        hooks: Optional[HookConfig] = None,
        retry=None,
        metrics: Optional[MetricsCollector] = None,
    ):
        self.name = name
        self.checkpoint = Checkpoint(name=name, checkpoint_dir=checkpoint_dir)
        self.filters = filters
        self.schema = schema
        self.transform = transform
        self.sink = sink
        self.logger = logger
        self.context = context
        self.hooks = hooks
        self.retry = retry
        self.metrics = metrics

    def _passes_filters(self, item: Dict) -> bool:
        if self.filters is None:
            return True
        return self.filters.should_process(item)

    def _passes_schema(self, item: Dict) -> bool:
        if self.schema is None:
            return True
        errors = self.schema.validate(item)
        return len(errors) == 0

    def _process_item(self, item: Dict) -> Optional[Dict]:
        if not self._passes_filters(item):
            return None
        if not self._passes_schema(item):
            return None
        if self.transform:
            try:
                item = self.transform.apply(item)
            except Exception:
                return None
        if self.sink:
            self.sink.emit(item)
        return item

    def run(self, items: Iterable[Dict]) -> List[Dict]:
        if self.context:
            self.context.start()
        if self.hooks:
            self.hooks.fire_start()

        seen = self.checkpoint.load()
        results = []

        for item in items:
            item_id = str(item.get("id", id(item)))
            if item_id in seen:
                continue
            if self.hooks:
                self.hooks.fire_item(item)
            try:
                result = self._process_item(item)
                if result is not None:
                    results.append(result)
                    seen.add(item_id)
                    if self.metrics:
                        self.metrics.increment("processed")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error processing item {item_id}: {e}")
                if self.hooks:
                    self.hooks.fire_error(e)

        self.checkpoint.save(seen)
        if self.hooks:
            self.hooks.fire_end()
        if self.context:
            self.context.stop()
        return results
