"""Integration tests for BatchPipeline with retry support."""

import os
import shutil
import pytest
from unittest.mock import MagicMock

from batchflow.pipeline import BatchPipeline
from batchflow.retry import RetryConfig

TEST_CHECKPOINT_DIR = ".test_pipeline_retry_checkpoints"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_CHECKPOINT_DIR):
        shutil.rmtree(TEST_CHECKPOINT_DIR)


class TestBatchPipelineRetry:
    def test_pipeline_runs_successfully(self):
        processor = lambda x: x ** 2
        pipeline = BatchPipeline(
            name="squares",
            processor=processor,
            checkpoint_dir=TEST_CHECKPOINT_DIR,
        )
        results = pipeline.run([1, 2, 3, 4])
        assert results == [1, 4, 9, 16]

    def test_pipeline_retries_on_failure(self):
        mock_processor = MagicMock(side_effect=[ValueError("oops"), 10, 20, 30])
        cfg = RetryConfig(max_attempts=2, delay=0)
        pipeline = BatchPipeline(
            name="retry_test",
            processor=mock_processor,
            checkpoint_dir=TEST_CHECKPOINT_DIR,
            retry_config=cfg,
        )
        results = pipeline.run(["a", "b", "c"])
        assert results == [10, 20, 30]
        assert mock_processor.call_count == 4

    def test_pipeline_raises_after_exhausting_retries(self):
        mock_processor = MagicMock(side_effect=RuntimeError("always fails"))
        cfg = RetryConfig(max_attempts=2, delay=0)
        pipeline = BatchPipeline(
            name="fail_test",
            processor=mock_processor,
            checkpoint_dir=TEST_CHECKPOINT_DIR,
            retry_config=cfg,
        )
        with pytest.raises(RuntimeError):
            pipeline.run(["x"])

    def test_pipeline_resumes_from_checkpoint(self):
        call_count = {"n": 0}

        def processor(item):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("Simulated crash")
            return item * 10

        pipeline = BatchPipeline(
            name="resume_retry",
            processor=processor,
            checkpoint_dir=TEST_CHECKPOINT_DIR,
            retry_config=RetryConfig(max_attempts=1, delay=0),
        )

        with pytest.raises(RuntimeError):
            pipeline.run([1, 2, 3], resume=False)

        pipeline2 = BatchPipeline(
            name="resume_retry",
            processor=lambda x: x * 10,
            checkpoint_dir=TEST_CHECKPOINT_DIR,
        )
        results = pipeline2.run([1, 2, 3], resume=True)
        assert results == [10, 20, 30]
