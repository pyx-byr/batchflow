import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.ratelimit import RateLimiter

CHECKPOINT_DIR = "/tmp/test_pipeline_ratelimit"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineRateLimit:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, limiter=None):
        return BatchPipeline(
            checkpoint_dir=CHECKPOINT_DIR,
            rate_limiter=limiter,
        )

    def test_pipeline_accepts_rate_limiter(self):
        limiter = RateLimiter(name="test", max_calls=10, period=1.0)
        pipeline = self._make_pipeline(limiter=limiter)
        assert pipeline.rate_limiter is limiter

    def test_pipeline_runs_without_rate_limiter(self):
        pipeline = self._make_pipeline()
        items = [1, 2, 3]
        results = pipeline.run(items, processor=lambda x: x)
        assert results == [1, 2, 3]

    def test_pipeline_runs_with_unlimited_limiter(self):
        limiter = RateLimiter(max_calls=0)
        pipeline = self._make_pipeline(limiter=limiter)
        items = list(range(5))
        results = pipeline.run(items, processor=lambda x: x * 2)
        assert results == [0, 2, 4, 6, 8]

    def test_pipeline_runs_with_high_rate_limit(self):
        limiter = RateLimiter(max_calls=100, period=1.0)
        pipeline = self._make_pipeline(limiter=limiter)
        items = ["a", "b", "c"]
        results = pipeline.run(items, processor=lambda x: x.upper())
        assert results == ["A", "B", "C"]
