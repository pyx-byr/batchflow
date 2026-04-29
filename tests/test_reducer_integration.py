import pytest
from batchflow.reducer import ReducerConfig


class TestReducerIntegration:
    def test_running_total_across_items(self):
        r = ReducerConfig()
        r.add("total", lambda acc, x: acc + x["value"], initial=0)
        items = [{"value": 10}, {"value": 20}, {"value": 30}]
        for item in items:
            r.reduce("total", item)
        assert r.result("total") == 60

    def test_max_value_reducer(self):
        r = ReducerConfig()
        r.add("max", lambda acc, x: x if x > acc else acc)
        for v in [3, 1, 4, 1, 5, 9, 2, 6]:
            r.reduce("max", v)
        assert r.result("max") == 9

    def test_collect_into_list(self):
        r = ReducerConfig()
        r.add("items", lambda acc, x: acc + [x], initial=[])
        for v in ["a", "b", "c"]:
            r.reduce("items", v)
        assert r.result("items") == ["a", "b", "c"]

    def test_reset_and_reuse(self):
        r = ReducerConfig()
        r.add("sum", lambda acc, x: acc + x, initial=0)
        for v in range(5):
            r.reduce("sum", v)
        assert r.result("sum") == 10
        r.reset("sum")
        for v in range(3):
            r.reduce("sum", v)
        assert r.result("sum") == 3

    def test_multiple_independent_reducers(self):
        r = ReducerConfig()
        r.add("count", lambda acc, x: acc + 1, initial=0)
        r.add("total", lambda acc, x: acc + x, initial=0)
        for v in [10, 20, 30]:
            r.reduce("count", v)
            r.reduce("total", v)
        res = r.results()
        assert res["count"] == 3
        assert res["total"] == 60

    def test_string_join_reducer(self):
        r = ReducerConfig()
        r.add("joined", lambda acc, x: acc + ", " + x if acc else x)
        for word in ["alpha", "beta", "gamma"]:
            r.reduce("joined", word)
        assert r.result("joined") == "alpha, beta, gamma"
