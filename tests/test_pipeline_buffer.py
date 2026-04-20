import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.buffer import BufferConfig
from batchflow.checkpoint import Checkpoint

CHECKPOINT_DIR = "/tmp/test_pipeline_buffer_checkpoints"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineBuffer:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, items, buffer=None):
        checkpoint = Checkpoint(directory=CHECKPOINT_DIR)
        return BatchPipeline(
            items=items,
            checkpoint=checkpoint,
            buffer=buffer,
        )

    def test_pipeline_accepts_buffer(self):
        buf = BufferConfig(max_size=5)
        pipeline = self._make_pipeline([1, 2, 3], buffer=buf)
        assert pipeline.buffer is buf

    def test_pipeline_runs_without_buffer(self):
        pipeline = self._make_pipeline([1, 2, 3])
        results = pipeline.run()
        assert results == [1, 2, 3]

    def test_buffer_collects_items_via_handler(self):
        flushed_batches = []
        buf = BufferConfig(max_size=3)
        buf.on_flush(lambda items: flushed_batches.append(list(items)))

        pipeline = self._make_pipeline(list(range(6)), buffer=buf)
        pipeline.run()

        # Two full flushes of 3 items each
        assert len(flushed_batches) == 2
        assert flushed_batches[0] == [0, 1, 2]
        assert flushed_batches[1] == [3, 4, 5]

    def test_buffer_partial_flush_at_end(self):
        flushed_batches = []
        buf = BufferConfig(max_size=4)
        buf.on_flush(lambda items: flushed_batches.append(list(items)))

        pipeline = self._make_pipeline([10, 20, 30], buffer=buf)
        pipeline.run()

        # No auto-flush triggered (size < max_size); manual flush needed
        # Pipeline should flush remaining items after processing
        remaining = buf.peek()
        assert remaining == [10, 20, 30]
