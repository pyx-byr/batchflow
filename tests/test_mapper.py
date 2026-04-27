import pytest
from batchflow.mapper import MapperConfig, apply_mapper


class TestMapperConfig:
    def test_defaults(self):
        m = MapperConfig()
        assert m.name == "mapper"
        assert m.mapping_count == 0

    def test_custom_name(self):
        m = MapperConfig(name="my_mapper")
        assert m.name == "my_mapper"

    def test_add_mapping(self):
        m = MapperConfig()
        m.add("age", int)
        assert m.mapping_count == 1
        assert "age" in m.keys()

    def test_add_chaining(self):
        m = MapperConfig()
        result = m.add("x", str).add("y", float)
        assert result is m
        assert m.mapping_count == 2

    def test_add_non_callable_raises(self):
        m = MapperConfig()
        with pytest.raises(TypeError):
            m.add("field", "not_callable")

    def test_map_applies_function(self):
        m = MapperConfig()
        m.add("score", lambda x: x * 2)
        result = m.map({"score": 5, "name": "alice"})
        assert result["score"] == 10
        assert result["name"] == "alice"

    def test_map_ignores_missing_keys(self):
        m = MapperConfig()
        m.add("missing_key", str)
        result = m.map({"other": 42})
        assert result == {"other": 42}

    def test_map_does_not_mutate_original(self):
        m = MapperConfig()
        m.add("val", lambda x: x + 1)
        original = {"val": 10}
        result = m.map(original)
        assert original["val"] == 10
        assert result["val"] == 11

    def test_map_non_dict_raises(self):
        m = MapperConfig()
        m.add("x", str)
        with pytest.raises(TypeError):
            m.map("not a dict")

    def test_reset_clears_mappings(self):
        m = MapperConfig()
        m.add("a", str).add("b", int)
        m.reset()
        assert m.mapping_count == 0
        assert m.keys() == []

    def test_keys_returns_registered_keys(self):
        m = MapperConfig()
        m.add("x", str).add("y", int).add("z", float)
        assert set(m.keys()) == {"x", "y", "z"}


class TestApplyMapper:
    def test_none_mapper_returns_item_unchanged(self):
        item = {"a": 1}
        assert apply_mapper(None, item) is item

    def test_empty_mapper_returns_item_unchanged(self):
        m = MapperConfig()
        item = {"a": 1}
        assert apply_mapper(m, item) is item

    def test_applies_mapper_when_configured(self):
        m = MapperConfig()
        m.add("val", lambda x: x * 3)
        result = apply_mapper(m, {"val": 4})
        assert result["val"] == 12
