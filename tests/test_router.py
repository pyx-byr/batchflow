import pytest
from batchflow.router import RouterConfig, apply_router


class TestRouterConfig:
    def test_defaults(self):
        r = RouterConfig()
        assert r.name == "router"
        assert r.route_labels() == []
        assert r._default is None

    def test_custom_name(self):
        r = RouterConfig(name="my_router")
        assert r.name == "my_router"

    def test_add_route_chaining(self):
        r = RouterConfig()
        result = r.add_route("evens", lambda x: x % 2 == 0)
        assert result is r
        assert "evens" in r.route_labels()

    def test_set_default_chaining(self):
        r = RouterConfig()
        result = r.set_default("other")
        assert result is r
        assert r._default == "other"

    def test_route_matches_first(self):
        r = RouterConfig()
        r.add_route("evens", lambda x: x % 2 == 0)
        r.add_route("odds", lambda x: x % 2 != 0)
        assert r.route(2) == "evens"
        assert r.route(3) == "odds"

    def test_route_returns_default_when_no_match(self):
        r = RouterConfig()
        r.add_route("big", lambda x: x > 100)
        r.set_default("small")
        assert r.route(5) == "small"

    def test_route_returns_none_without_default(self):
        r = RouterConfig()
        r.add_route("big", lambda x: x > 100)
        assert r.route(5) is None

    def test_route_skips_erroring_predicate(self):
        r = RouterConfig()
        r.add_route("bad", lambda x: x["key"])  # will raise on int
        r.add_route("good", lambda x: isinstance(x, int))
        assert r.route(42) == "good"


class TestApplyRouter:
    def test_partitions_items(self):
        r = RouterConfig()
        r.add_route("evens", lambda x: x % 2 == 0)
        r.add_route("odds", lambda x: x % 2 != 0)
        result = apply_router([1, 2, 3, 4], r)
        assert result["evens"] == [2, 4]
        assert result["odds"] == [1, 3]

    def test_unmatched_items_excluded_without_default(self):
        r = RouterConfig()
        r.add_route("big", lambda x: x > 10)
        result = apply_router([1, 2, 20], r)
        assert result == {"big": [20]}

    def test_unmatched_items_go_to_default(self):
        r = RouterConfig()
        r.add_route("big", lambda x: x > 10)
        r.set_default("small")
        result = apply_router([1, 20], r)
        assert result["big"] == [20]
        assert result["small"] == [1]

    def test_empty_input(self):
        r = RouterConfig()
        r.add_route("any", lambda x: True)
        assert apply_router([], r) == {}
