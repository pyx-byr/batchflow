import logging
import pytest
import os
from batchflow.pipeline import BatchPipeline
from batchflow.logging import PipelineLogger
from batchflow.checkpoint import Checkpoint

CHECKPOINT_PATH = "/tmp/test_pipeline_logging_checkpoint"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    cp = Checkpoint(CHECKPOINT_PATH)
    cp.clear()


class TestPipelineLogging:
    def _make_pipeline(self, items, logger=None):
        cp = Checkpoint(CHECKPOINT_PATH)
        cp.clear()
        return BatchPipeline(items=items, checkpoint=cp, logger=logger)

    def test_pipeline_accepts_logger(self, capfd):
        logger = PipelineLogger(name="pipe_log_test")
        pipeline = self._make_pipeline([1, 2, 3], logger=logger)
        results = pipeline.run()
        assert results == [1, 2, 3]

    def test_pipeline_runs_without_logger(self):
        pipeline = self._make_pipeline(["a", "b"])
        results = pipeline.run()
        assert results == ["a", "b"]

    def test_logger_output_contains_items(self, capfd):
        logger = PipelineLogger(name="pipe_log_output")
        pipeline = self._make_pipeline([10, 20], logger=logger)
        pipeline.run()
        out, _ = capfd.readouterr()
        assert "10" in out or "20" in out
