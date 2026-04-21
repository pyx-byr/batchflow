import os
import pytest
from batchflow.pipeline import BatchPipeline
from batchflow.enricher import EnricherConfig

CHECKPOINT_DIR = "/tmp/test_pipeline_enricher_checkpoints"


def cleanup():
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)


class TestPipelineEnricher:
    def setup_method(self):
        cleanup()

    def teardown_method(self):
        cleanup()

    def _make_pipeline(self, enricher=None):
        return BatchPipeline(
            name="test",
            checkpoint_dir=CHECKPOINT_DIR,
            enricher=enricher,
        )

    def test_pipeline_accepts_enricher(self):
        enricher = EnricherConfig().add("score", lambda item: item.get("value", 0) * 10)
        pipeline = self._make_pipeline(enricher=enricher)
        assert pipeline.enricher is enricher

    def test_pipeline_runs_without_enricher(self):
        pipeline = self._make_pipeline()
        results = []
        pipeline.run(
            items=[{"id": 1}],
            processor=lambda item: item,
            sink=lambda item: results.append(item),
        )
        assert len(results) == 1

    def test_enricher_fields_present_in_output(self):
        enricher = (
            EnricherConfig()
            .add("label", lambda item: f"item_{item['id']}")
            .add("doubled", lambda item: item["id"] * 2)
        )
        pipeline = self._make_pipeline(enricher=enricher)
        results = []
        pipeline.run(
            items=[{"id": 3}, {"id": 7}],
            processor=lambda item: item,
            sink=lambda item: results.append(item),
        )
        assert results[0]["label"] == "item_3"
        assert results[0]["doubled"] == 6
        assert results[1]["label"] == "item_7"
        assert results[1]["doubled"] == 14
