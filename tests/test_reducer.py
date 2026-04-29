import pytest
from batchflow.reducer import ReducerConfig, apply_reducer


class TestReducerConfig:
    def test_defaults(self):
        r = ReducerConfig()
        assert r.name == "reducer"
        assert r.reducer_count() == 0

    def test_custom_name(self):
        r = ReducerConfig(name="my_reducer")
        assert r.name == "my_reducer"

    def test_add_reducer(self):
        r = ReducerConfig()
        r.add("sum", lambda acc, x: acc + x, initial=0)
        assert r.reducer_count() == 1

    def test_add_chaining(self):
        r = ReducerConfig()
        result = r.add("sum", lambda acc, x: acc + x, initial=0)
        assert result is r

    def test_add_non_callable_raises(self):
        r = ReducerConfig()
        with pytest.raises(TypeError, match="must be callable"):
            r.add("bad", "not_a_function")

    def test_reduce_sum(self):
        r = ReducerConfig()
        r.add("sum", lambda acc, x: acc + x, initial=0)
        r.reduce("sum", 10)
        r.reduce("sum", 20)
        r.reduce("sum", 5)
        assert r.result("sum") == 35

    def test_reduce_no_initial_uses_first_item(self):
        r = ReducerConfig()
        r.add("concat", lambda acc, x: acc + x)
        r.reduce("concat", "hello")
        r.reduce("concat", " world")
        assert r.result("concat") == "hello world"

    def test_reduce_unknown_label_raises(self):
        r = ReducerConfig()
        with pytest.raises(KeyError, match="No reducer registered"):
            r.reduce("missing", 1)

    def test_result_unknown_label_raises(self):
        r = ReducerConfig()
        with pytest.raises(KeyError):
            r.result("missing")

    def test_results_multiple(self):
        r = ReducerConfig()
        r.add("sum", lambda acc, x: acc + x, initial=0)
        r.add("product", lambda acc, x: acc * x, initial=1)
        r.reduce("sum", 3)
        r.reduce("product", 4)
        res = r.results()
        assert res["sum"] == 3
        assert res["product"] == 4

    def test_reset_single(self):
        r = ReducerConfig()
        r.add("sum", lambda acc, x: acc + x, initial=0)
        r.reduce("sum", 99)
        r.reset("sum")
        assert r.result("sum") == 0

    def test_reset_all(self):
        r = ReducerConfig()
        r.add("sum", lambda acc, x: acc + x, initial=0)
        r.add("product", lambda acc, x: acc * x, initial=1)
        r.reduce("sum", 5)
        r.reduce("product", 5)
        r.reset()
        assert r.result("sum") == 0
        assert r.result("product") == 1

    def test_reset_unknown_label_raises(self):
        r = ReducerConfig()
        with pytest.raises(KeyError):
            r.reset("ghost")


class TestApplyReducer:
    def test_none_reducer_returns_item(self):
        assert apply_reducer(None, "sum", 42) == 42

    def test_applies_reducer(self):
        r = ReducerConfig()
        r.add("sum", lambda acc, x: acc + x, initial=0)
        result = apply_reducer(r, "sum", 10)
        assert result == 10
