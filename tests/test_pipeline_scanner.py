import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.scanner import ScannerConfig
from batchflow.checkpoint import Checkpoint


CHECKPOINT_DIR = "/tmp/test_pipeline_scanner_checkpoints"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineScanner:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, scanner=None):
        checkpoint = Checkpoint(directory=CHECKPOINT_DIR, name="scanner_test")
        return BatchPipeline(
            name="scanner_pipeline",
            checkpoint=checkpoint,
            scanner=scanner,
        )

    def test_pipeline_accepts_scanner(self):
        scanner = ScannerConfig(name="test_scanner")
        pipeline = self._make_pipeline(scanner=scanner)
        assert pipeline.scanner is scanner

    def test_pipeline_runs_without_scanner(self):
        pipeline = self._make_pipeline(scanner=None)
        items = [1, 2, 3]
        results = []
        pipeline.run(items, handler=lambda x: results.append(x))
        assert results == [1, 2, 3]

    def test_pipeline_scanner_accumulates_state(self):
        scanner = ScannerConfig()
        scanner.add("total", lambda acc, x: acc + x, initial=0)
        pipeline = self._make_pipeline(scanner=scanner)
        items = [10, 20, 30]
        pipeline.run(items, handler=lambda x: None)
        assert scanner.get_state("total") == 60

    def test_pipeline_scanner_does_not_alter_items(self):
        scanner = ScannerConfig()
        scanner.add("count", lambda acc, x: acc + 1, initial=0)
        pipeline = self._make_pipeline(scanner=scanner)
        items = ["a", "b", "c"]
        results = []
        pipeline.run(items, handler=lambda x: results.append(x))
        assert results == ["a", "b", "c"]
        assert scanner.get_state("count") == 3
