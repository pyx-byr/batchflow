import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.mapper import MapperConfig
from batchflow.sink import SinkConfig

CHECKPOINT_DIR = ".test_checkpoints_mapper"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineMapper:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, mapper=None):
        sink = SinkConfig()
        collected = []
        sink.add(collected.append)
        return BatchPipeline(
            items=[{"val": 1}, {"val": 2}, {"val": 3}],
            checkpoint_dir=CHECKPOINT_DIR,
            mapper=mapper,
            sink=sink,
        ), collected

    def test_pipeline_accepts_mapper(self):
        mapper = MapperConfig().add("val", lambda x: x * 10)
        pipeline, collected = self._make_pipeline(mapper=mapper)
        assert pipeline.mapper is mapper

    def test_pipeline_runs_without_mapper(self):
        pipeline, collected = self._make_pipeline(mapper=None)
        pipeline.run()
        assert len(collected) == 3

    def test_mapper_transforms_items_in_pipeline(self):
        mapper = MapperConfig().add("val", lambda x: x + 100)
        pipeline, collected = self._make_pipeline(mapper=mapper)
        pipeline.run()
        values = [item["val"] for item in collected]
        assert values == [101, 102, 103]

    def test_mapper_only_affects_mapped_fields(self):
        items = [{"val": 1, "label": "a"}, {"val": 2, "label": "b"}]
        mapper = MapperConfig().add("val", lambda x: x * 2)
        sink = SinkConfig()
        collected = []
        sink.add(collected.append)
        pipeline = BatchPipeline(
            items=items,
            checkpoint_dir=CHECKPOINT_DIR,
            mapper=mapper,
            sink=sink,
        )
        pipeline.run()
        assert collected[0]["label"] == "a"
        assert collected[0]["val"] == 2
        assert collected[1]["label"] == "b"
        assert collected[1]["val"] == 4
