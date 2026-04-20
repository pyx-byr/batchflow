import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.event import EventBus

CHECKPOINT_DIR = "/tmp/test_pipeline_events"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineEvents:
    def _make_pipeline(self, items, event_bus=None):
        return BatchPipeline(
            items=items,
            checkpoint_dir=CHECKPOINT_DIR,
            event_bus=event_bus,
        )

    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def test_pipeline_accepts_event_bus(self):
        bus = EventBus(name="test")
        pipeline = self._make_pipeline([1, 2, 3], event_bus=bus)
        assert pipeline.event_bus is bus

    def test_pipeline_runs_without_event_bus(self):
        pipeline = self._make_pipeline([1, 2, 3])
        results = pipeline.run(lambda x: x * 2)
        assert results == [2, 4, 6]

    def test_item_processed_event_fired(self):
        bus = EventBus()
        fired = []
        bus.subscribe("item.processed", lambda p: fired.append(p))
        pipeline = self._make_pipeline([10, 20], event_bus=bus)
        pipeline.run(lambda x: x + 1)
        assert len(fired) == 2

    def test_pipeline_start_end_events(self):
        bus = EventBus()
        events = []
        bus.subscribe("pipeline.start", lambda p: events.append("start"))
        bus.subscribe("pipeline.end", lambda p: events.append("end"))
        pipeline = self._make_pipeline([1], event_bus=bus)
        pipeline.run(lambda x: x)
        assert events == ["start", "end"]
