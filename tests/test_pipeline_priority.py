import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.priority import PriorityConfig
from batchflow.checkpoint import Checkpoint

CHECKPOINT_DIR = "/tmp/test_pipeline_priority_checkpoints"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelinePriority:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, priority: PriorityConfig = None):
        checkpoint = Checkpoint(checkpoint_dir=CHECKPOINT_DIR)
        return BatchPipeline(
            checkpoint=checkpoint,
            priority=priority,
        )

    def test_pipeline_accepts_priority(self):
        p = PriorityConfig()
        pipeline = self._make_pipeline(priority=p)
        assert pipeline.priority is p

    def test_pipeline_runs_without_priority(self):
        pipeline = self._make_pipeline()
        items = [1, 2, 3]
        results = list(pipeline.run(items))
        assert results == [1, 2, 3]

    def test_pipeline_processes_in_priority_order(self):
        p = PriorityConfig()
        p.set_key(lambda x: x["rank"])
        pipeline = self._make_pipeline(priority=p)
        items = [
            {"rank": 3, "val": "c"},
            {"rank": 1, "val": "a"},
            {"rank": 2, "val": "b"},
        ]
        results = list(pipeline.run(items))
        assert [r["val"] for r in results] == ["a", "b", "c"]

    def test_pipeline_priority_reverse(self):
        p = PriorityConfig(reverse=True)
        p.set_key(lambda x: x)
        pipeline = self._make_pipeline(priority=p)
        items = [1, 3, 2]
        results = list(pipeline.run(items))
        assert results == [3, 2, 1]
