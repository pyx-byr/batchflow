import pytest
from batchflow.schema import SchemaConfig, SchemaField, apply_schema


class TestSchemaConfig:
    def test_defaults(self):
        s = SchemaConfig()
        assert s.fields == []
        assert s.name == "schema"
        assert s.strict is False

    def test_custom_name(self):
        s = SchemaConfig(name="my_schema")
        assert s.name == "my_schema"

    def test_add_field(self):
        s = SchemaConfig()
        result = s.add("id", int)
        assert len(s.fields) == 1
        assert s.fields[0].name == "id"
        assert result is s

    def test_add_chaining(self):
        s = SchemaConfig().add("id", int).add("name", str)
        assert len(s.fields) == 2

    def test_validate_valid_item(self):
        s = SchemaConfig().add("id", int).add("name", str)
        errors = s.validate({"id": 1, "name": "alice"})
        assert errors == []

    def test_validate_missing_required(self):
        s = SchemaConfig().add("id", int)
        errors = s.validate({})
        assert any("Missing required field" in e for e in errors)

    def test_validate_optional_missing_ok(self):
        s = SchemaConfig().add("id", int, required=False)
        errors = s.validate({})
        assert errors == []

    def test_validate_wrong_type(self):
        s = SchemaConfig().add("id", int)
        errors = s.validate({"id": "not_an_int"})
        assert any("expected int" in e for e in errors)

    def test_validate_custom_validator_pass(self):
        s = SchemaConfig().add("age", int, validator=lambda x: x >= 0)
        errors = s.validate({"age": 25})
        assert errors == []

    def test_validate_custom_validator_fail(self):
        s = SchemaConfig().add("age", int, validator=lambda x: x >= 0)
        errors = s.validate({"age": -1})
        assert any("custom validation" in e for e in errors)

    def test_strict_mode_rejects_extra(self):
        s = SchemaConfig(strict=True).add("id", int)
        errors = s.validate({"id": 1, "extra": "oops"})
        assert any("Unexpected field" in e for e in errors)

    def test_strict_mode_off_allows_extra(self):
        s = SchemaConfig(strict=False).add("id", int)
        errors = s.validate({"id": 1, "extra": "ok"})
        assert errors == []


class TestApplySchema:
    def test_none_schema_returns_empty(self):
        assert apply_schema({"id": 1}, None) == []

    def test_with_schema(self):
        s = SchemaConfig().add("id", int)
        assert apply_schema({"id": 1}, s) == []
