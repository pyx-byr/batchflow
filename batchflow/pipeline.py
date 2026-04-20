from typing import Any, List, Optional
from batchflow.checkpoint import Checkpoint
from batchflow.filter import FilterConfig
from batchflow.schema import SchemaConfig
from batchflow.transform import TransformConfig
from batchflow.sink import SinkConfig
from batchflow.retry import RetryConfig, with_retry
from batchflow.logging import PipelineLogger
from batchflow.context import PipelineContext
from batchflow.hook import HookConfig
from batchflow.metrics import MetricsCollector
from batchflow.event import EventBus
from batchflow.ratelimit import RateLimiter
from batchflow.pause import PauseControl
from batchflow.timeout import TimeoutConfig, apply_timeout


class BatchPipeline:
    def __init__(
        self,
        items: List[Any],
        checkpoint: Checkpoint,
        filters: Optional[FilterConfig] = None,
        schema: Optional[SchemaConfig] = None,
        transform: Optional[TransformConfig] = None,
        sink: Optional[SinkConfig] = None,
        retry: Optional[RetryConfig] = None,
        logger: Optional[PipelineLogger] = None,
        context: Optional[PipelineContext] = None,
        hooks: Optional[HookConfig] = None,
        metrics: Optional[MetricsCollector] = None,
        event_bus: Optional[EventBus] = None,
        rate_limiter: Optional[RateLimiter] = None,
        pause_control: Optional[PauseControl] = None,
        timeout: Optional[TimeoutConfig] = None,
    ):
        self.items = items
        self.checkpoint = checkpoint
        self.filters = filters
        self.schema = schema
        self.transform = transform
        self.sink = sink
        self.retry = retry
        self.logger = logger
        self.context = context
        self.hooks = hooks
        self.metrics = metrics
        self.event_bus = event_bus
        self.rate_limiter = rate_limiter
        self.pause_control = pause_control
        self.timeout = timeout

    def _passes_filters(self, item: Any) -> bool:
        if self.filters is None:
            return True
        return self.filters.should_process(item)

    def _passes_schema(self, item: Any) -> bool:
        if self.schema is None:
            return True
        errors = self.schema.validate(item)
        return len(errors) == 0

    def _process_item(self, item: Any) -> Any:
        if self.transform:
            item = apply_timeout(self.timeout, self.transform.apply, item)
        return item

    def run(self) -> List[Any]:
        processed = self.checkpoint.load()
        seen_ids = {r["id"] for r in processed if "id" in r} if processed else set()
        results = []

        if self.context:
            self.context.start()
        if self.hooks:
            self.hooks.fire_start()
        if self.event_bus:
            self.event_bus.publish("pipeline.start", {})

        for item in self.items:
            item_id = str(item)
            if item_id in seen_ids:
                continue
            if not self._passes_filters(item):
                continue
            if not self._passes_schema(item):
                continue
            if self.rate_limiter:
                self.rate_limiter.acquire()
            if self.pause_control:
                self.pause_control.wait_if_paused()
            try:
                if self.retry:
                    fn = with_retry(self.retry)(self._process_item)
                    result = fn(item)
                else:
                    result = self._process_item(item)
                results.append(result)
                if self.sink:
                    self.sink.emit(result)
                if self.hooks:
                    self.hooks.fire_item(result)
                if self.metrics:
                    self.metrics.increment("processed")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed item {item_id}: {e}")
                if self.metrics:
                    self.metrics.increment("failed")
                if self.hooks:
                    self.hooks.fire_error(e)

        self.checkpoint.save([{"id": str(r)} for r in results])

        if self.hooks:
            self.hooks.fire_end()
        if self.context:
            self.context.stop()
        if self.event_bus:
            self.event_bus.publish("pipeline.end", {"count": len(results)})

        return results
