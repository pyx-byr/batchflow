import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.checkpoint import Checkpoint
from batchflow.schema import SchemaConfig

CHECKPOINT_PATH = "/tmp/test_pipeline_schema_checkpoint"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    cp = Checkpoint(CHECKPOINT_PATH)
    cp.clear()


class TestPipelineSchema:
    def _make_pipeline(self, items, schema=None):
        cp = Checkpoint(CHECKPOINT_PATH)
        return BatchPipeline(items=items, checkpoint=cp, schema=schema)

    def test_pipeline_accepts_schema(self):
        schema = SchemaConfig().add("id", int)
        pipeline = self._make_pipeline([{"id": 1}], schema=schema)
        assert pipeline.schema is schema

    def test_pipeline_runs_without_schema(self):
        pipeline = self._make_pipeline([{"id": 1}])
        results = pipeline.run()
        assert len(results) == 1

    def test_pipeline_skips_invalid_items(self):
        schema = SchemaConfig().add("id", int)
        items = [{"id": 1}, {"id": "bad"}, {"id": 3}]
        pipeline = self._make_pipeline(items, schema=schema)
        results = pipeline.run()
        assert len(results) == 2
        assert all(isinstance(r["id"], int) for r in results)

    def test_pipeline_skips_missing_required(self):
        schema = SchemaConfig().add("id", int).add("name", str)
        items = [{"id": 1, "name": "alice"}, {"id": 2}]
        pipeline = self._make_pipeline(items, schema=schema)
        results = pipeline.run()
        assert len(results) == 1
