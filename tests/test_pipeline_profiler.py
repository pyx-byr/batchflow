import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.profiler import ProfilerConfig
from batchflow.sink import SinkConfig

CHECKPOINT_DIR = "/tmp/test_pipeline_profiler_checkpoints"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineProfiler:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, profiler=None):
        sink = SinkConfig()
        collected = []
        sink.add(collected.append)
        return (
            BatchPipeline(
                source=[1, 2, 3],
                checkpoint_dir=CHECKPOINT_DIR,
                profiler=profiler,
            ),
            sink,
            collected,
        )

    def test_pipeline_accepts_profiler(self):
        profiler = ProfilerConfig(name="test_profiler")
        pipeline, sink, collected = self._make_pipeline(profiler=profiler)
        assert pipeline.profiler is profiler

    def test_pipeline_runs_without_profiler(self):
        pipeline, sink, collected = self._make_pipeline(profiler=None)
        pipeline.run(lambda item: sink.emit(item))
        assert collected == [1, 2, 3]

    def test_pipeline_runs_with_profiler(self):
        profiler = ProfilerConfig()
        pipeline, sink, collected = self._make_pipeline(profiler=profiler)
        pipeline.run(lambda item: sink.emit(item))
        assert collected == [1, 2, 3]

    def test_profiler_is_none_by_default(self):
        pipeline = BatchPipeline(
            source=[],
            checkpoint_dir=CHECKPOINT_DIR,
        )
        assert pipeline.profiler is None

    def test_profiler_summary_accessible_after_run(self):
        profiler = ProfilerConfig()
        pipeline, sink, collected = self._make_pipeline(profiler=profiler)
        # manually profile a stage to verify summary is accessible
        profiler.start("preprocess")
        profiler.stop("preprocess")
        summary = profiler.summary()
        assert "preprocess" in summary
        assert summary["preprocess"]["count"] == 1
