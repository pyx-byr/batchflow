import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.transform import TransformConfig
from batchflow.sink import SinkConfig, collect_to_list
from batchflow.checkpoint import Checkpoint

CHECKPOINT_FILE = "/tmp/test_transform_sink_pipeline.json"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


class TestTransformSinkPipeline:
    def _make_pipeline(self, items, transform, sink):
        checkpoint = Checkpoint(CHECKPOINT_FILE)
        return BatchPipeline(
            items=items,
            processor=lambda item: sink.emit(transform.apply(item)),
            checkpoint=checkpoint,
        )

    def test_transform_and_collect(self):
        results = []
        tc = TransformConfig()
        tc.add(lambda x: x * 2)
        sc = SinkConfig()
        sc.add(collect_to_list(results))
        pipeline = self._make_pipeline([1, 2, 3], tc, sc)
        pipeline.run()
        assert results == [2, 4, 6]

    def test_skip_on_transform_error(self):
        results = []
        tc = TransformConfig(skip_on_error=True)
        tc.add(lambda x: 1 // x)
        sc = SinkConfig()
        sc.add(collect_to_list(results))

        checkpoint = Checkpoint(CHECKPOINT_FILE)
        pipeline = BatchPipeline(
            items=[2, 0, 4],
            processor=lambda item: (
                None if tc.apply(item) is None else sc.emit(tc.apply(item))
            ),
            checkpoint=checkpoint,
        )
        pipeline.run()
        # 2 -> 0 (int div), 0 -> None (skipped), 4 -> 0
        assert len(results) == 2

    def test_checkpoint_resumes(self):
        results = []
        tc = TransformConfig()
        tc.add(lambda x: x + 10)
        sc = SinkConfig()
        sc.add(collect_to_list(results))

        checkpoint = Checkpoint(CHECKPOINT_FILE)
        checkpoint.save(["a", "b"])

        pipeline = BatchPipeline(
            items=["a", "b", "c"],
            processor=lambda item: sc.emit(tc.apply(item)),
            checkpoint=checkpoint,
        )
        pipeline.run()
        assert results == ["c10"]
