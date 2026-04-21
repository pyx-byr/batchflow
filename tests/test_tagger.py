import pytest
from batchflow.tagger import TaggerConfig, apply_tagger


class TestTaggerConfig:
    def test_defaults(self):
        t = TaggerConfig()
        assert t.name == "tagger"
        assert t.tags() == []

    def test_custom_name(self):
        t = TaggerConfig(name="my_tagger")
        assert t.name == "my_tagger"

    def test_add_tagger(self):
        t = TaggerConfig()
        t.add("even", lambda x: x % 2 == 0)
        assert "even" in t.tags()

    def test_add_chaining(self):
        t = TaggerConfig()
        result = t.add("big", lambda x: x > 100).add("small", lambda x: x < 10)
        assert result is t
        assert set(t.tags()) == {"big", "small"}

    def test_add_non_callable_raises(self):
        t = TaggerConfig()
        with pytest.raises(TypeError, match="must be callable"):
            t.add("bad", "not_a_function")

    def test_tag_returns_matching_tags(self):
        t = TaggerConfig()
        t.add("even", lambda x: x % 2 == 0)
        t.add("big", lambda x: x > 50)
        assert t.tag(100) == ["even", "big"]
        assert t.tag(3) == []
        assert t.tag(4) == ["even"]

    def test_has_tag_true(self):
        t = TaggerConfig()
        t.add("negative", lambda x: x < 0)
        assert t.has_tag(-5, "negative") is True

    def test_has_tag_false(self):
        t = TaggerConfig()
        t.add("negative", lambda x: x < 0)
        assert t.has_tag(5, "negative") is False

    def test_has_tag_unknown_label(self):
        t = TaggerConfig()
        assert t.has_tag(42, "unknown") is False

    def test_clear_removes_all(self):
        t = TaggerConfig()
        t.add("a", lambda x: True).add("b", lambda x: False)
        t.clear()
        assert t.tags() == []

    def test_clear_chaining(self):
        t = TaggerConfig()
        result = t.clear()
        assert result is t


class TestApplyTagger:
    def test_apply_with_none(self):
        assert apply_tagger(42, None) == []

    def test_apply_with_tagger(self):
        t = TaggerConfig()
        t.add("even", lambda x: x % 2 == 0)
        assert apply_tagger(4, t) == ["even"]
        assert apply_tagger(3, t) == []

    def test_apply_multiple_tags(self):
        t = TaggerConfig()
        t.add("positive", lambda x: x > 0)
        t.add("small", lambda x: x < 10)
        result = apply_tagger(5, t)
        assert "positive" in result
        assert "small" in result
