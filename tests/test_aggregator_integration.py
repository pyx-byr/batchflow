import pytest
from batchflow.aggregator import AggregatorConfig


class TestAggregatorIntegration:
    """Integration tests combining multiple aggregation labels and workflows."""

    def test_multi_label_pipeline_simulation(self):
        agg = AggregatorConfig(name="pipeline_stats")
        agg.add("sum", sum)
        agg.add("count", len)
        agg.add("max", lambda items: max(items) if items else None)
        agg.add("min", lambda items: min(items) if items else None)

        data = [3, 1, 4, 1, 5, 9, 2, 6]
        for item in data:
            for label in ["sum", "count", "max", "min"]:
                agg.collect(label, item)

        r = agg.results()
        assert r["sum"] == 31
        assert r["count"] == 8
        assert r["max"] == 9
        assert r["min"] == 1

    def test_reset_and_reuse(self):
        agg = AggregatorConfig()
        agg.add("total", sum)

        for v in [1, 2, 3]:
            agg.collect("total", v)
        assert agg.result("total") == 6

        agg.reset()
        assert agg.result("total") == 0

        for v in [10, 20]:
            agg.collect("total", v)
        assert agg.result("total") == 30

    def test_custom_aggregation_fn(self):
        agg = AggregatorConfig()
        agg.add("avg", lambda items: sum(items) / len(items) if items else 0.0)
        for v in [10, 20, 30]:
            agg.collect("avg", v)
        assert agg.result("avg") == 20.0

    def test_empty_bucket_result(self):
        agg = AggregatorConfig()
        agg.add("sum", sum)
        assert agg.result("sum") == 0

    def test_collect_chaining(self):
        agg = AggregatorConfig()
        agg.add("sum", sum)
        result = agg.collect("sum", 5).collect("sum", 10)
        assert result is agg
        assert agg.result("sum") == 15

    def test_reset_chaining(self):
        agg = AggregatorConfig()
        agg.add("sum", sum)
        agg.collect("sum", 99)
        result = agg.reset("sum")
        assert result is agg
        assert agg.result("sum") == 0
