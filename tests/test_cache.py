import pytest
from batchflow.cache import CacheConfig, apply_cache


class TestCacheConfig:
    def test_defaults(self):
        c = CacheConfig()
        assert c.name == "cache"
        assert c.max_size == 128
        assert c.size == 0
        assert c.hits == 0
        assert c.misses == 0

    def test_custom_name(self):
        c = CacheConfig(name="my_cache", max_size=64)
        assert c.name == "my_cache"
        assert c.max_size == 64

    def test_set_and_get(self):
        c = CacheConfig()
        c.set({"id": 1}, "result_1")
        assert c.get({"id": 1}) == "result_1"

    def test_has(self):
        c = CacheConfig()
        assert not c.has("key")
        c.set("key", "value")
        assert c.has("key")

    def test_miss_increments(self):
        c = CacheConfig()
        c.get("nonexistent")
        assert c.misses == 1
        assert c.hits == 0

    def test_hit_increments(self):
        c = CacheConfig()
        c.set("k", "v")
        c.get("k")
        assert c.hits == 1
        assert c.misses == 0

    def test_clear(self):
        c = CacheConfig()
        c.set("k", "v")
        c.get("k")
        c.clear()
        assert c.size == 0
        assert c.hits == 0
        assert c.misses == 0

    def test_max_size_evicts_oldest(self):
        c = CacheConfig(max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)
        assert c.size == 2
        assert not c.has("a")
        assert c.has("b")
        assert c.has("c")


class TestApplyCache:
    def test_caches_result(self):
        c = CacheConfig()
        calls = []

        def fn(item):
            calls.append(item)
            return item * 2

        result1 = apply_cache(5, fn, c)
        result2 = apply_cache(5, fn, c)
        assert result1 == 10
        assert result2 == 10
        assert len(calls) == 1

    def test_different_items_call_fn(self):
        c = CacheConfig()
        calls = []

        def fn(item):
            calls.append(item)
            return item + 1

        apply_cache(1, fn, c)
        apply_cache(2, fn, c)
        assert len(calls) == 2
