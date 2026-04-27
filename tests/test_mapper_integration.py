import pytest
from batchflow.mapper import MapperConfig, apply_mapper
from batchflow.transform import TransformConfig
from batchflow.enricher import EnricherConfig


class TestMapperIntegration:
    def test_mapper_after_enricher(self):
        """Mapper can process fields added by an enricher."""
        enricher = EnricherConfig()
        enricher.add(lambda item: {**item, "score": 10})

        mapper = MapperConfig()
        mapper.add("score", lambda x: x * 2)

        item = {"id": 1}
        enriched = enricher.enrich(item)
        mapped = mapper.map(enriched)

        assert mapped["score"] == 20
        assert mapped["id"] == 1

    def test_mapper_chained_with_transform(self):
        """Mapper field transforms compose correctly with item-level transforms."""
        transform = TransformConfig()
        transform.add(lambda item: {**item, "processed": True})

        mapper = MapperConfig()
        mapper.add("value", lambda x: str(x).upper())

        item = {"value": "hello"}
        transformed = transform.apply(item)
        mapped = mapper.map(transformed)

        assert mapped["value"] == "HELLO"
        assert mapped["processed"] is True

    def test_multiple_field_mappings(self):
        mapper = MapperConfig()
        mapper.add("first", str.upper)
        mapper.add("second", lambda x: x ** 2)
        mapper.add("third", lambda x: round(x, 2))

        item = {"first": "hello", "second": 5, "third": 3.14159}
        result = mapper.map(item)

        assert result["first"] == "HELLO"
        assert result["second"] == 25
        assert result["third"] == 3.14

    def test_apply_mapper_with_none_is_identity(self):
        item = {"key": "value"}
        assert apply_mapper(None, item) is item

    def test_reset_and_reuse(self):
        mapper = MapperConfig()
        mapper.add("x", lambda v: v + 1)
        assert mapper.map({"x": 1})["x"] == 2

        mapper.reset()
        assert mapper.mapping_count == 0

        mapper.add("x", lambda v: v * 10)
        assert mapper.map({"x": 2})["x"] == 20

    def test_mapper_with_type_coercion(self):
        mapper = MapperConfig()
        mapper.add("age", int)
        mapper.add("score", float)
        mapper.add("name", str.strip)

        item = {"age": "25", "score": "9.5", "name": "  alice  "}
        result = mapper.map(item)

        assert result["age"] == 25
        assert result["score"] == 9.5
        assert result["name"] == "alice"
