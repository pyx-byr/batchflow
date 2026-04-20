import os
from typing import Any, Callable, Iterable, List, Optional
from batchflow.checkpoint import Checkpoint
from batchflow.filter import FilterConfig
from batchflow.schema import SchemaConfig
from batchflow.retry import RetryConfig, with_retry
from batchflow.logging import PipelineLogger
from batchflow.context import PipelineContext
from batchflow.hook import HookConfig
from batchflow.metrics import MetricsCollector
from batchflow.event import EventBus


class BatchPipeline:
    def __init__(
        self,
        items: Iterable,
        checkpoint_dir: str,
        filters: Optional[FilterConfig] = None,
        schema: Optional[SchemaConfig] = None,
        retry: Optional[RetryConfig] = None,
        logger: Optional[PipelineLogger] = None,
        context: Optional[PipelineContext] = None,
        hooks: Optional[HookConfig] = None,
        metrics: Optional[MetricsCollector] = None,
        event_bus: Optional[EventBus] = None,
    ):
        self.items = list(items)
        self.checkpoint = Checkpoint(checkpoint_dir)
        self.filters = filters
        self.schema = schema
        self.retry = retry
        self.logger = logger
        self.context = context
        self.hooks = hooks
        self.metrics = metrics
        self.event_bus = event_bus

    def _passes_filters(self, item: Any) -> bool:
        if self.filters is None:
            return True
        return self.filters.should_process(item)

    def _passes_schema(self, item: Any) -> bool:
        if self.schema is None:
            return True
        errors = self.schema.validate(item)
        return len(errors) == 0

    def _process_item(self, fn: Callable, item: Any) -> Any:
        if self.retry:
            return with_retry(self.retry)(fn)(item)
        return fn(item)

    def run(self, fn: Callable) -> List[Any]:
        if self.context:
            self.context.start()
        if self.hooks:
            self.hooks.fire_start()
        if self.event_bus:
            self.event_bus.publish("pipeline.start")

        results = []
        processed_ids = self.checkpoint.load()

        for item in self.items:
            item_id = str(item)
            if item_id in processed_ids:
                if self.logger:
                    self.logger.info(f"Skipping already processed item: {item}")
                continue
            if not self._passes_filters(item):
                continue
            if not self._passes_schema(item):
                continue
            try:
                result = self._process_item(fn, item)
                results.append(result)
                processed_ids.add(item_id)
                self.checkpoint.save(processed_ids)
                if self.hooks:
                    self.hooks.fire_item(item)
                if self.metrics:
                    self.metrics.increment("processed")
                if self.event_bus:
                    self.event_bus.publish("item.processed", {"item": item, "result": result})
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error processing item {item}: {e}")
                if self.hooks:
                    self.hooks.fire_error(e)
                if self.metrics:
                    self.metrics.increment("failed")
                raise

        if self.hooks:
            self.hooks.fire_end()
        if self.context:
            self.context.stop()
        if self.event_bus:
            self.event_bus.publish("pipeline.end", {"total": len(results)})

        return results
