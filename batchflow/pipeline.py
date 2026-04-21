from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, List, Optional

from batchflow.checkpoint import Checkpoint
from batchflow.filter import FilterConfig, apply_filters
from batchflow.schema import SchemaConfig, apply_schema
from batchflow.enricher import EnricherConfig, apply_enricher


@dataclass
class BatchPipeline:
    """Resumable batch processing pipeline with checkpointing."""

    name: str = "pipeline"
    checkpoint_dir: str = "/tmp/batchflow_checkpoints"
    filters: Optional[FilterConfig] = None
    schema: Optional[SchemaConfig] = None
    enricher: Optional[EnricherConfig] = None
    retry: Any = None
    logger: Any = None
    context: Any = None
    hooks: Any = None
    metrics: Any = None
    event_bus: Any = None
    rate_limiter: Any = None
    pause_control: Any = None
    timeout: Any = None
    buffer: Any = None
    priority: Any = None
    _extra: dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        self._checkpoint = Checkpoint(name=self.name, checkpoint_dir=self.checkpoint_dir)

    def __setattr__(self, name, value):
        # Allow arbitrary keyword arguments to be stored without breaking dataclass
        try:
            super().__setattr__(name, value)
        except AttributeError:
            object.__getattribute__(self, "_extra")[name] = value

    def __getattr__(self, name):
        extra = object.__getattribute__(self, "_extra")
        if name in extra:
            return extra[name]
        raise AttributeError(name)

    def _passes_filters(self, item: Any) -> bool:
        if self.filters is None:
            return True
        return apply_filters(item, self.filters)

    def _passes_schema(self, item: Any) -> bool:
        if self.schema is None:
            return True
        errors = apply_schema(item, self.schema)
        return len(errors) == 0

    def _enrich_item(self, item: Any) -> Any:
        return apply_enricher(item, self.enricher)

    def _process_item(
        self,
        item: Any,
        processor: Callable[[Any], Any],
        sink: Optional[Callable[[Any], None]],
    ) -> bool:
        try:
            if not self._passes_filters(item):
                return False
            if not self._passes_schema(item):
                return False
            item = self._enrich_item(item)
            result = processor(item)
            if sink is not None and result is not None:
                sink(result)
            return True
        except Exception as exc:
            if self.logger is not None:
                self.logger.error(f"Error processing item {item!r}: {exc}")
            return False

    def run(
        self,
        items: Iterable[Any],
        processor: Callable[[Any], Any],
        sink: Optional[Callable[[Any], None]] = None,
        id_fn: Optional[Callable[[Any], str]] = None,
    ) -> None:
        """Process *items* one by one, skipping already-checkpointed ids."""
        if self.hooks is not None:
            self.hooks.fire_start()
        if self.context is not None:
            self.context.start()

        for item in items:
            item_id = id_fn(item) if id_fn else None
            if item_id is not None and self._checkpoint.exists(item_id):
                continue

            success = self._process_item(item, processor, sink)

            if success and item_id is not None:
                self._checkpoint.save(item_id)

        if self.context is not None:
            self.context.stop()
        if self.hooks is not None:
            self.hooks.fire_end()
