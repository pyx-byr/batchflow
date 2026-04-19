import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.context import PipelineContext
from batchflow.checkpoint import Checkpoint

CHECKPOINT_PATH = "/tmp/test_ctx_pipeline.json"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)


class TestPipelineContext:
    def _make_pipeline(self, items, context=None):
        return BatchPipeline(
            items=items,
            context=context,
        )

    def test_pipeline_accepts_context(self):
        ctx = PipelineContext(run_id="run-1")
        pipeline = self._make_pipeline([1, 2, 3], context=ctx)
        results = pipeline.run()
        assert results == [1, 2, 3]

    def test_context_elapsed_after_run(self):
        ctx = PipelineContext(run_id="run-2")
        pipeline = self._make_pipeline(list(range(5)), context=ctx)
        pipeline.run()
        assert ctx.elapsed is not None
        assert ctx.elapsed >= 0

    def test_context_metadata_accessible_after_run(self):
        ctx = PipelineContext(run_id="run-3")
        ctx.set("source", "test-data")
        pipeline = self._make_pipeline([10, 20], context=ctx)
        pipeline.run()
        assert ctx.get("source") == "test-data"

    def test_pipeline_runs_without_context(self):
        pipeline = self._make_pipeline([1, 2, 3])
        results = pipeline.run()
        assert results == [1, 2, 3]

    def test_context_tags_preserved(self):
        ctx = PipelineContext(run_id="run-4")
        ctx.tag("env", "test").tag("version", "1.0")
        pipeline = self._make_pipeline(["a", "b"], context=ctx)
        pipeline.run()
        assert ctx.tags["env"] == "test"
        assert ctx.tags["version"] == "1.0"
