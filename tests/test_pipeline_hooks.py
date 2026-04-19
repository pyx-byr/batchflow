import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.hook import HookConfig
from batchflow.checkpoint import Checkpoint

CHECKPOINT_DIR = "/tmp/test_pipeline_hooks_cp"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    cp = Checkpoint(CHECKPOINT_DIR, "hooks_test")
    cp.clear()


class TestPipelineHooks:
    def _make_pipeline(self, items, hooks=None):
        cp = Checkpoint(CHECKPOINT_DIR, "hooks_test")
        return BatchPipeline(items=items, checkpoint=cp, hooks=hooks)

    def test_pipeline_accepts_hooks(self):
        hooks = HookConfig()
        pipeline = self._make_pipeline([{"id": 1}], hooks=hooks)
        assert pipeline.hooks is hooks

    def test_on_start_called(self):
        called = []
        hooks = HookConfig()
        hooks.on_start(lambda: called.append("start"))
        pipeline = self._make_pipeline([{"id": 1}], hooks=hooks)
        pipeline.run()
        assert "start" in called

    def test_on_end_called(self):
        called = []
        hooks = HookConfig()
        hooks.on_end(lambda: called.append("end"))
        pipeline = self._make_pipeline([{"id": 1}], hooks=hooks)
        pipeline.run()
        assert "end" in called

    def test_on_item_called_per_item(self):
        seen = []
        hooks = HookConfig()
        hooks.on_item(lambda x: seen.append(x["id"]))
        items = [{"id": 1}, {"id": 2}, {"id": 3}]
        pipeline = self._make_pipeline(items, hooks=hooks)
        pipeline.run()
        assert seen == [1, 2, 3]

    def test_pipeline_runs_without_hooks(self):
        pipeline = self._make_pipeline([{"id": 1}])
        results = pipeline.run()
        assert len(results) == 1
