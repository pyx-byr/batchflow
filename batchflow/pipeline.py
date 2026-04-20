from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Iterable, List, Optional

from batchflow.checkpoint import Checkpoint
from batchflow.filter import FilterConfig, apply_filters
from batchflow.schema import SchemaConfig, apply_schema
from batchflow.transform import TransformConfig, apply_transforms
from batchflow.sink import SinkConfig
from batchflow.retry import RetryConfig, with_retry
from batchflow.logging import PipelineLogger
from batchflow.context import PipelineContext
from batchflow.metrics import MetricsCollector
from batchflow.hook import HookConfig
from batchflow.event import EventBus
from batchflow.ratelimit import RateLimiter
from batchflow.pause import PauseControl
from batchflow.timeout import TimeoutConfig, apply_timeout
from batchflow.buffer import BufferConfig
from batchflow.priority import PriorityConfig, apply_priority


@dataclass
class BatchPipeline:
    """A resumable batch data processing pipeline with checkpointing."""

    checkpoint: Checkpoint
    filters: Optional[FilterConfig] = None
    schema: Optional[SchemaConfig] = None
    transforms: Optional[TransformConfig] = None
    sink: Optional[SinkConfig] = None
    retry: Optional[RetryConfig] = None
    logger: Optional[PipelineLogger] = None
    context: Optional[PipelineContext] = None
    metrics: Optional[MetricsCollector] = None
    hooks: Optional[HookConfig] = None
    events: Optional[EventBus] = None
    rate_limiter: Optional[RateLimiter] = None
    pause_control: Optional[PauseControl] = None
    timeout: Optional[TimeoutConfig] = None
    buffer: Optional[BufferConfig] = None
    priority: Optional[PriorityConfig] = None

    def _passes_filters(self, item: Any) -> bool:
        if self.filters is None:
            return True
        return apply_filters(item, self.filters)

    def _passes_schema(self, item: Any) -> bool:
        if self.schema is None:
            return True
        errors = apply_schema(item, self.schema)
        return len(errors) == 0

    def _process_item(self, item: Any) -> Any:
        if self.transforms:
            item = apply_transforms(item, self.transforms)
        return item

    def run(self, items: Iterable[Any]) -> Generator[Any, None, None]:
        """Process items through the pipeline, yielding results."""
        if self.context:
            self.context.start()
        if self.hooks:
            self.hooks.fire_start()
        if self.events:
            self.events.publish("pipeline.start", {})

        processed_ids = self.checkpoint.load()

        item_list = list(items)

        # Apply priority ordering if configured
        if self.priority:
            item_list = apply_priority(item_list, self.priority)

        for item in item_list:
            item_id = str(item)

            if item_id in processed_ids:
                continue

            if self._control:
                self.pause_control.wait_if_paused()

            if self.rate_limiter:
                self.rate_limiter.acquire()

            if not self._passes_filters(item):
                continue

            if not self._passes_schema(item):
                continue

            try:
                if self.retry:
                    fn = with_retry(self.retry)(self._process_item)
                    result = fn(item)
                elif self.timeout and self.timeout.enabled:
                    result = apply_timeout(self._process_item, item, self.timeout)
                else:
                    result = self._process_item(item)
            except Exception as exc:
                if self.logger:
                    self.logger.error(f"Failed to process item {item_id}: {exc}")
                if self.hooks:
                    self.hooks.fire_error(exc)
                if self.events:
                    self.events.publish("pipeline.error", {"item": item, "error": exc})
                continue

            if self.sink:
                self.sink.emit(result)

            if self.buffer:
                self.buffer.add(result)

            if self.hooks:
                self.hooks.fire_item(result)

            if self.events:
                self.events.publish("pipeline.item", {"item": result})

            if self.metrics:
                self.metrics.increment("items_processed")

            processed_ids.add(item_id)
            self.checkpoint.save(processed_ids)

            yield result

        if self.buffer and not self.buffer.is_empty():
            self.buffer.flush()

        if self.context:
            self.context.stop()
        if self.hooks:
            self.hooks.fire_end()
        if self.events:
            self.events.publish("pipeline.end", {})
