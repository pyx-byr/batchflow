import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.limiter import LimiterConfig
from batchflow.checkpoint import Checkpoint

CHECKPOINT_DIR = "/tmp/test_pipeline_limiter"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineLimiter:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, items, limiter=None):
        cp = Checkpoint(run_id="limiter-test", directory=CHECKPOINT_DIR)
        return BatchPipeline(
            items=items,
            checkpoint=cp,
            limiter=limiter,
        )

    def test_pipeline_accepts_limiter(self):
        lim = LimiterConfig(max_items=2)
        pipeline = self._make_pipeline([1, 2, 3], limiter=lim)
        assert pipeline.limiter is lim

    def test_pipeline_runs_without_limiter(self):
        results = []
        pipeline = self._make_pipeline([1, 2, 3])
        pipeline.sink.add(lambda item: results.append(item))
        pipeline.run()
        assert results == [1, 2, 3]

    def test_pipeline_limits_items_processed(self):
        results = []
        lim = LimiterConfig(max_items=2)
        pipeline = self._make_pipeline([10, 20, 30, 40], limiter=lim)
        pipeline.sink.add(lambda item: results.append(item))
        pipeline.run()
        assert results == [10, 20]

    def test_pipeline_limit_of_one(self):
        results = []
        lim = LimiterConfig(max_items=1)
        pipeline = self._make_pipeline(["a", "b", "c"], limiter=lim)
        pipeline.sink.add(lambda item: results.append(item))
        pipeline.run()
        assert results == ["a"]

    def test_pipeline_limit_larger_than_input(self):
        results = []
        lim = LimiterConfig(max_items=100)
        pipeline = self._make_pipeline([1, 2, 3], limiter=lim)
        pipeline.sink.add(lambda item: results.append(item))
        pipeline.run()
        assert results == [1, 2, 3]
