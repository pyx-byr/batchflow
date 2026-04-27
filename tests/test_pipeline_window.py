import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.window import WindowConfig
from batchflow.checkpoint import Checkpoint

CHECKPOINT_DIR = "/tmp/test_pipeline_window_checkpoints"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineWindow:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, window: WindowConfig):
        checkpoint = Checkpoint(directory=CHECKPOINT_DIR)
        return BatchPipeline(
            items=list(range(9)),
            processor=lambda x: x,
            checkpoint=checkpoint,
            window=window,
        )

    def test_pipeline_accepts_window(self):
        w = WindowConfig(size=3, step=3)
        pipeline = self._make_pipeline(w)
        assert pipeline.window is w

    def test_pipeline_runs_without_window(self):
        checkpoint = Checkpoint(directory=CHECKPOINT_DIR)
        pipeline = BatchPipeline(
            items=[1, 2, 3],
            processor=lambda x: x,
            checkpoint=checkpoint,
        )
        results = pipeline.run()
        assert results == [1, 2, 3]

    def test_pipeline_window_collects_windows(self):
        collected = []
        w = WindowConfig(size=3, step=3)
        w.on_window(lambda win: collected.append(list(win)))
        checkpoint = Checkpoint(directory=CHECKPOINT_DIR)
        pipeline = BatchPipeline(
            items=list(range(9)),
            processor=lambda x: x,
            checkpoint=checkpoint,
            window=w,
        )
        pipeline.run()
        assert collected == [[0, 1, 2], [3, 4, 5], [6, 7, 8]]
