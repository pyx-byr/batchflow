import pytest
from batchflow.enricher import EnricherConfig, apply_enricher


class TestEnricherConfig:
    def test_defaults(self):
        e = EnricherConfig()
        assert e.name == "enricher"
        assert e.enricher_count == 0

    def test_custom_name(self):
        e = EnricherConfig(name="my_enricher")
        assert e.name == "my_enricher"

    def test_add_enricher(self):
        e = EnricherConfig()
        e.add("doubled", lambda item: item["value"] * 2)
        assert e.enricher_count == 1

    def test_add_chaining(self):
        e = EnricherConfig()
        result = e.add("a", lambda item: 1).add("b", lambda item: 2)
        assert result is e
        assert e.enricher_count == 2

    def test_add_non_callable_raises(self):
        e = EnricherConfig()
        with pytest.raises(TypeError, match="callable"):
            e.add("key", "not_a_function")

    def test_enrich_adds_fields(self):
        e = EnricherConfig()
        e.add("upper", lambda item: item["name"].upper())
        result = e.enrich({"name": "alice"})
        assert result["upper"] == "ALICE"

    def test_enrich_multiple_fields(self):
        e = EnricherConfig()
        e.add("x2", lambda item: item["v"] * 2)
        e.add("x3", lambda item: item["v"] * 3)
        result = e.enrich({"v": 5})
        assert result["x2"] == 10
        assert result["x3"] == 15

    def test_enrich_uses_default_on_error(self):
        e = EnricherConfig()
        e.add("safe", lambda item: item["missing_key"], default="fallback")
        result = e.enrich({"v": 1})
        assert result["safe"] == "fallback"

    def test_enrich_non_dict_raises(self):
        e = EnricherConfig()
        e.add("k", lambda item: item)
        with pytest.raises(TypeError, match="dict"):
            e.enrich([1, 2, 3])

    def test_reset(self):
        e = EnricherConfig()
        e.add("k", lambda item: 1)
        e.reset()
        assert e.enricher_count == 0

    def test_reset_chaining(self):
        e = EnricherConfig()
        result = e.add("k", lambda item: 1).reset()
        assert result is e


class TestApplyEnricher:
    def test_none_enricher_returns_item(self):
        item = {"a": 1}
        assert apply_enricher(item, None) is item

    def test_applies_enricher(self):
        e = EnricherConfig().add("flag", lambda item: True)
        result = apply_enricher({"x": 0}, e)
        assert result["flag"] is True
