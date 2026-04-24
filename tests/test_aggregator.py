import pytest
from batchflow.aggregator import AggregatorConfig, apply_aggregator


class TestAggregatorConfig:
    def test_defaults(self):
        agg = AggregatorConfig()
        assert agg.name == "aggregator"
        assert agg.aggregator_count == 0

    def test_custom_name(self):
        agg = AggregatorConfig(name="my_agg")
        assert agg.name == "my_agg"

    def test_add_aggregator(self):
        agg = AggregatorConfig()
        agg.add("sum", sum)
        assert agg.aggregator_count == 1

    def test_add_chaining(self):
        agg = AggregatorConfig()
        result = agg.add("sum", sum).add("count", len)
        assert result is agg
        assert agg.aggregator_count == 2

    def test_collect_and_result(self):
        agg = AggregatorConfig()
        agg.add("sum", sum)
        agg.collect("sum", 1)
        agg.collect("sum", 2)
        agg.collect("sum", 3)
        assert agg.result("sum") == 6

    def test_result_unknown_label_raises(self):
        agg = AggregatorConfig()
        with pytest.raises(KeyError, match="no_label"):
            agg.result("no_label")

    def test_results_multiple_labels(self):
        agg = AggregatorConfig()
        agg.add("sum", sum).add("count", len)
        for v in [10, 20, 30]:
            agg.collect("sum", v)
            agg.collect("count", v)
        r = agg.results()
        assert r["sum"] == 60
        assert r["count"] == 3

    def test_reset_single_label(self):
        agg = AggregatorConfig()
        agg.add("sum", sum)
        agg.collect("sum", 5)
        agg.reset("sum")
        assert agg.result("sum") == 0

    def test_reset_all(self):
        agg = AggregatorConfig()
        agg.add("sum", sum).add("count", len)
        agg.collect("sum", 1)
        agg.collect("count", 1)
        agg.reset()
        assert agg.result("sum") == 0
        assert agg.result("count") == 0

    def test_collect_auto_creates_bucket(self):
        agg = AggregatorConfig()
        agg.add("max", max)
        agg.collect("max", 7)
        assert agg.result("max") == 7


class TestApplyAggregator:
    def test_returns_item_when_none(self):
        assert apply_aggregator(None, "x", 42) == 42

    def test_collects_item(self):
        agg = AggregatorConfig()
        agg.add("total", sum)
        apply_aggregator(agg, "total", 10)
        apply_aggregator(agg, "total", 20)
        assert agg.result("total") == 30
