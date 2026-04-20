import pytest
from batchflow.validator import ValidatorConfig, apply_validator


class TestValidatorConfig:
    def test_defaults(self):
        v = ValidatorConfig()
        assert v.name == "validator"
        assert v.raise_on_error is False
        assert v._validators == []

    def test_custom_name(self):
        v = ValidatorConfig(name="my_validator")
        assert v.name == "my_validator"

    def test_add_validator(self):
        v = ValidatorConfig()
        v.add(lambda x: x > 0, "must be positive")
        assert len(v._validators) == 1

    def test_add_chaining(self):
        v = ValidatorConfig()
        result = v.add(lambda x: x > 0).add(lambda x: x < 100)
        assert result is v
        assert len(v._validators) == 2

    def test_validate_passes(self):
        v = ValidatorConfig()
        v.add(lambda x: x > 0, "must be positive")
        ok, msg = v.validate(5)
        assert ok is True
        assert msg is None

    def test_validate_fails(self):
        v = ValidatorConfig()
        v.add(lambda x: x > 0, "must be positive")
        ok, msg = v.validate(-1)
        assert ok is False
        assert msg == "must be positive"

    def test_is_valid_true(self):
        v = ValidatorConfig()
        v.add(lambda x: isinstance(x, int))
        assert v.is_valid(42) is True

    def test_is_valid_false(self):
        v = ValidatorConfig()
        v.add(lambda x: isinstance(x, int))
        assert v.is_valid("hello") is False

    def test_raise_on_error(self):
        v = ValidatorConfig(raise_on_error=True)
        v.add(lambda x: x > 0, "must be positive")
        with pytest.raises(ValueError, match="must be positive"):
            v.validate(-1)

    def test_exception_in_fn_returns_false(self):
        v = ValidatorConfig()
        v.add(lambda x: x["key"] > 0, "key check")
        ok, msg = v.validate("not a dict")
        assert ok is False
        assert "key check" in msg


class TestApplyValidator:
    def test_none_validator(self):
        ok, msg = apply_validator({"x": 1}, None)
        assert ok is True
        assert msg is None

    def test_with_validator(self):
        v = ValidatorConfig()
        v.add(lambda x: x.get("age", 0) >= 18, "must be adult")
        ok, msg = apply_validator({"age": 10}, v)
        assert ok is False
        assert msg == "must be adult"
