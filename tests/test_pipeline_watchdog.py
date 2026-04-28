"""Integration tests for WatchdogConfig with BatchPipeline."""

import os
import pytest
from batchflow.watchdog import WatchdogConfig
from batchflow.pipeline import BatchPipeline
from batchflow.checkpoint import Checkpoint


CHECKPOINT_DIR = "/tmp/test_pipeline_watchdog"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineWatchdog:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, watchdog=None):
        checkpoint = Checkpoint(name="watchdog_test", directory=CHECKPOINT_DIR)
        return BatchPipeline(
            name="watchdog_pipeline",
            checkpoint=checkpoint,
            watchdog=watchdog,
        )

    def test_pipeline_accepts_watchdog(self):
        wd = WatchdogConfig(name="wd", timeout_seconds=5.0)
        pipeline = self._make_pipeline(watchdog=wd)
        assert pipeline.watchdog is wd

    def test_pipeline_runs_without_watchdog(self):
        pipeline = self._make_pipeline(watchdog=None)
        items = [1, 2, 3]
        results = []
        pipeline.run(items, processor=lambda x: results.append(x))
        assert results == [1, 2, 3]

    def test_pipeline_runs_with_watchdog(self):
        stalls = []
        wd = WatchdogConfig(timeout_seconds=5.0, on_stall=lambda e: stalls.append(e))
        pipeline = self._make_pipeline(watchdog=wd)
        items = ["a", "b", "c"]
        results = []
        pipeline.run(items, processor=lambda x: results.append(x))
        assert results == ["a", "b", "c"]
        assert stalls == []

    def test_watchdog_no_false_stall_on_fast_pipeline(self):
        stalls = []
        wd = WatchdogConfig(timeout_seconds=10.0, on_stall=lambda e: stalls.append(e))
        pipeline = self._make_pipeline(watchdog=wd)
        items = list(range(10))
        pipeline.run(items, processor=lambda x: x)
        assert stalls == []
