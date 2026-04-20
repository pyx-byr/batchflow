import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.pause import PauseControl
from batchflow.checkpoint import Checkpoint

CHECKPOINT_DIR = "/tmp/test_pipeline_pause"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelinePause:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, pause_control=None):
        checkpoint = Checkpoint(checkpoint_dir=CHECKPOINT_DIR, run_id="pause_test")
        return BatchPipeline(
            checkpoint=checkpoint,
            pause_control=pause_control,
        )

    def test_pipeline_accepts_pause_control(self):
        pc = PauseControl()
        pipeline = self._make_pipeline(pause_control=pc)
        assert pipeline.pause_control is pc

    def test_pipeline_runs_without_pause_control(self):
        pipeline = self._make_pipeline()
        items = [1, 2, 3]
        results = list(pipeline.run(items))
        assert results == [1, 2, 3]

    def test_pipeline_runs_with_unpaused_control(self):
        pc = PauseControl()
        pipeline = self._make_pipeline(pause_control=pc)
        items = ["a", "b", "c"]
        results = list(pipeline.run(items))
        assert results == ["a", "b", "c"]

    def test_pipeline_pause_control_not_paused_by_default(self):
        pc = PauseControl()
        assert pc.is_paused is False
        pipeline = self._make_pipeline(pause_control=pc)
        results = list(pipeline.run([10, 20]))
        assert results == [10, 20]
