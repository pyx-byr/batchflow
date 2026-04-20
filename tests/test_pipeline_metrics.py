import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.metrics import MetricsCollector

CHECKPOINT_DIR = "/tmp/test_pipeline_metrics"


@pytest.fixture(autouse=True)
def cleanup():
    import shutil
    shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)
    yield
    shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)


def _make_pipeline(metrics=None):
    return BatchPipeline(
        name="metrics-test",
        checkpoint_dir=CHECKPOINT_DIR,
        metrics=metrics,
    )


class TestPipelineMetrics:
    def test_pipeline_accepts_metrics(self):
        m = MetricsCollector(name="my-metrics")
        pipeline = _make_pipeline(metrics=m)
        assert pipeline.metrics is m

    def test_pipeline_runs_without_metrics(self):
        pipeline = _make_pipeline(metrics=None)
        items = [{"id": i} for i in range(3)]
        results = pipeline.run(items)
        assert len(results) == 3

    def test_metrics_counts_processed(self):
        m = MetricsCollector()
        pipeline = _make_pipeline(metrics=m)
        items = [{"id": i} for i in range(5)]
        pipeline.run(items)
        assert m.get("processed") == 5

    def test_metrics_summary_after_run(self):
        m = MetricsCollector(name="summary-test")
        pipeline = _make_pipeline(metrics=m)
        items = [{"id": i} for i in range(4)]
        pipeline.run(items)
        s = m.summary()
        assert s["counts"]["processed"] == 4
