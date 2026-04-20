import pytest
from batchflow.dedupe import DedupeConfig, apply_dedupe


class TestDedupeConfig:
    def test_defaults(self):
        d = DedupeConfig()
        assert d.name == "dedupe"
        assert d.key_fn is None
        assert d.seen_count == 0

    def test_custom_name(self):
        d = DedupeConfig(name="my_dedupe")
        assert d.name == "my_dedupe"

    def test_is_duplicate_primitive(self):
        d = DedupeConfig()
        assert d.is_duplicate(1) is False
        assert d.is_duplicate(1) is True
        assert d.is_duplicate(2) is False

    def test_seen_count(self):
        d = DedupeConfig()
        d.is_duplicate("a")
        d.is_duplicate("b")
        d.is_duplicate("a")
        assert d.seen_count == 2

    def test_reset(self):
        d = DedupeConfig()
        d.is_duplicate("x")
        d.reset()
        assert d.seen_count == 0
        assert d.is_duplicate("x") is False

    def test_reset_chaining(self):
        d = DedupeConfig()
        result = d.reset()
        assert result is d

    def test_custom_key_fn(self):
        d = DedupeConfig(key_fn=lambda item: item["id"])
        assert d.is_duplicate({"id": 1, "val": "a"}) is False
        assert d.is_duplicate({"id": 1, "val": "b"}) is True
        assert d.is_duplicate({"id": 2, "val": "c"}) is False


class TestApplyDedupe:
    def test_none_dedupe_never_skips(self):
        assert apply_dedupe("anything", None) is False

    def test_skips_duplicate(self):
        d = DedupeConfig()
        assert apply_dedupe("hello", d) is False
        assert apply_dedupe("hello", d) is True

    def test_does_not_skip_unique(self):
        d = DedupeConfig()
        apply_dedupe(1, d)
        assert apply_dedupe(2, d) is False
