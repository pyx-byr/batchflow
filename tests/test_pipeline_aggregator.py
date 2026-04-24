import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.aggregator import AggregatorConfig

CHECKPOINT_DIR = ".test_pipeline_aggregator_checkpoints"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineAggregator:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, aggregator=None):
        return BatchPipeline(
            name="agg-pipe",
            source=[1, 2, 3, 4, 5],
            checkpoint_dir=CHECKPOINT_DIR,
            aggregator=aggregator,
        )

    def test_pipeline_accepts_aggregator(self):
        agg = AggregatorConfig()
        pipe = self._make_pipeline(aggregator=agg)
        assert pipe.aggregator is agg

    def test_pipeline_runs_without_aggregator(self):
        pipe = self._make_pipeline()
        results = pipe.run()
        assert results == [1, 2, 3, 4, 5]

    def test_aggregator_collects_during_run(self):
        agg = AggregatorConfig().add("total", sum)
        collected = []

        def collect_fn(item):
            agg.collect("total", item)
            collected.append(item)
            return item

        pipe = BatchPipeline(
            name="agg-pipe",
            source=[10, 20, 30],
            checkpoint_dir=CHECKPOINT_DIR,
            aggregator=agg,
            process_fn=collect_fn,
        )
        pipe.run()
        assert agg.result("total") == 60
        assert collected == [10, 20, 30]
