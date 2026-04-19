import os
import shutil
import pytest
from batchflow.checkpoint import Checkpoint
from batchflow.pipeline import BatchPipeline

TEST_DIR = ".test_checkpoints"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)


class TestCheckpoint:
    def test_save_and_load(self):
        cp = Checkpoint(TEST_DIR)
        cp.save("pipe1", 3, {"extra": "info"})
        data = cp.load("pipe1")
        assert data["batch_index"] == 3
        assert data["metadata"]["extra"] == "info"

    def test_exists(self):
        cp = Checkpoint(TEST_DIR)
        assert not cp.exists("pipe2")
        cp.save("pipe2", 0)
        assert cp.exists("pipe2")

    def test_clear(self):
        cp = Checkpoint(TEST_DIR)
        cp.save("pipe3", 1)
        cp.clear("pipe3")
        assert not cp.exists("pipe3")

    def test_load_missing_returns_none(self):
        cp = Checkpoint(TEST_DIR)
        assert cp.load("nonexistent") is None


class TestBatchPipeline:
    def test_basic_run(self):
        pipeline = BatchPipeline(
            "test", steps=[lambda x: x * 2], checkpoint_dir=TEST_DIR
        )
        data = range(10)
        results = list(pipeline.run(data, batch_size=4))
        flat = [item for batch in results for item in batch]
        assert flat == [i * 2 for i in range(10)]

    def test_checkpoint_cleared_after_completion(self):
        cp = Checkpoint(TEST_DIR)
        pipeline = BatchPipeline("done", steps=[lambda x: x], checkpoint_dir=TEST_DIR)
        list(pipeline.run(range(5), batch_size=5))
        assert not cp.exists("done")

    def test_resume_skips_completed_batches(self):
        processed = []
        pipeline = BatchPipeline(
            "resume_test",
            steps=[lambda x: processed.append(x) or x],
            checkpoint_dir=TEST_DIR,
        )
        # Simulate a checkpoint at batch 0 already done
        pipeline.checkpoint.save("resume_test", 0)
        list(pipeline.run(range(10), batch_size=5, resume=True))
        # Only second batch (indices 5-9) should be processed
        assert processed == list(range(5, 10))
