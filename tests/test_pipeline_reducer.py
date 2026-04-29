import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.reducer import ReducerConfig
from batchflow.checkpoint import Checkpoint


CHECKPOINT_PATH = "/tmp/test_pipeline_reducer"


def cleanup():
    cp = Checkpoint(CHECKPOINT_PATH)
    cp.clear()


class TestPipelineReducer:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, reducer=None):
        cp = Checkpoint(CHECKPOINT_PATH)
        return BatchPipeline(
            checkpoint=cp,
            reducer=reducer,
        )

    def test_pipeline_accepts_reducer(self):
        reducer = ReducerConfig()
        reducer.add("sum", lambda acc, x: acc + x, initial=0)
        pipeline = self._make_pipeline(reducer=reducer)
        assert pipeline.reducer is reducer

    def test_pipeline_runs_without_reducer(self):
        pipeline = self._make_pipeline(reducer=None)
        items = [1, 2, 3]
        results = []
        pipeline.run(
            items,
            fn=lambda x: results.append(x),
        )
        assert results == [1, 2, 3]

    def test_pipeline_with_reducer_accumulates(self):
        reducer = ReducerConfig()
        reducer.add("sum", lambda acc, x: acc + x, initial=0)
        pipeline = self._make_pipeline(reducer=reducer)
        items = [10, 20, 30]
        pipeline.run(
            items,
            fn=lambda x: reducer.reduce("sum", x),
        )
        assert reducer.result("sum") == 60

    def test_reducer_reset_between_runs(self):
        reducer = ReducerConfig()
        reducer.add("count", lambda acc, x: acc + 1, initial=0)
        pipeline = self._make_pipeline(reducer=reducer)
        items = ["a", "b", "c"]
        pipeline.run(items, fn=lambda x: reducer.reduce("count", x))
        assert reducer.result("count") == 3
        reducer.reset("count")
        pipeline.run(items[:2], fn=lambda x: reducer.reduce("count", x))
        assert reducer.result("count") == 2
