import time
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.timeout import TimeoutConfig
from batchflow.checkpoint import Checkpoint

CHECKPOINT_DIR = "/tmp/test_pipeline_timeout_checkpoints"


def cleanup():
    import shutil, os
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineTimeout:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, timeout: TimeoutConfig = None):
        cp = Checkpoint(CHECKPOINT_DIR, "timeout_test")
        return BatchPipeline(items=[1, 2, 3], checkpoint=cp, timeout=timeout)

    def test_pipeline_accepts_timeout(self):
        t = TimeoutConfig(seconds=5.0)
        pipeline = self._make_pipeline(timeout=t)
        assert pipeline.timeout is not None
        assert pipeline.timeout.seconds == 5.0

    def test_pipeline_runs_without_timeout(self):
        pipeline = self._make_pipeline()
        results = pipeline.run()
        assert len(results) == 3

    def test_pipeline_runs_within_timeout(self):
        t = TimeoutConfig(seconds=2.0)
        pipeline = self._make_pipeline(timeout=t)
        results = pipeline.run()
        assert len(results) == 3

    def test_pipeline_skips_on_timeout(self):
        t = TimeoutConfig(seconds=0.05, name="fast_timeout")
        cp = Checkpoint(CHECKPOINT_DIR, "timeout_skip")

        def slow_transform(item):
            time.sleep(0.5)
            return item

        from batchflow.transform import TransformConfig
        transform = TransformConfig().add(slow_transform)
        pipeline = BatchPipeline(
            items=[1, 2],
            checkpoint=cp,
            timeout=t,
            transform=transform,
        )
        results = pipeline.run()
        assert results == []

    def test_pipeline_timeout_name_defaults_to_none(self):
        """TimeoutConfig without a name should have name set to None."""
        t = TimeoutConfig(seconds=1.0)
        assert t.name is None

    def test_pipeline_timeout_name_is_preserved(self):
        """TimeoutConfig should preserve the provided name."""
        t = TimeoutConfig(seconds=1.0, name="my_timeout")
        pipeline = self._make_pipeline(timeout=t)
        assert pipeline.timeout.name == "my_timeout"
