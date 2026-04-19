import pytest
from batchflow.transform import TransformConfig, apply_transforms


class TestTransformConfig:
    def test_default_attributes(self):
        tc = TransformConfig()
        assert tc.name == "transform"
        assert tc.transforms == []
        assert tc.skip_on_error is False

    def test_custom_name(self):
        tc = TransformConfig(name="my_transform")
        assert tc.name == "my_transform"

    def test_add_transform(self):
        tc = TransformConfig()
        tc.add(str.upper)
        assert len(tc.transforms) == 1

    def test_add_chaining(self):
        tc = TransformConfig()
        result = tc.add(str.upper).add(str.strip)
        assert result is tc
        assert len(tc.transforms) == 2

    def test_apply_single(self):
        tc = TransformConfig()
        tc.add(lambda x: x * 2)
        assert tc.apply(5) == 10

    def test_apply_multiple(self):
        tc = TransformConfig()
        tc.add(lambda x: x + 1)
        tc.add(lambda x: x * 3)
        assert tc.apply(2) == 9

    def test_apply_no_transforms(self):
        tc = TransformConfig()
        assert tc.apply("hello") == "hello"

    def test_apply_error_raises(self):
        tc = TransformConfig(skip_on_error=False)
        tc.add(lambda x: 1 / x)
        with pytest.raises(RuntimeError):
            tc.apply(0)

    def test_apply_error_skip(self):
        tc = TransformConfig(skip_on_error=True)
        tc.add(lambda x: 1 / x)
        assert tc.apply(0) is None


class TestApplyTransforms:
    def test_delegates_to_config(self):
        tc = TransformConfig()
        tc.add(lambda x: x.upper())
        assert apply_transforms("hello", tc) == "HELLO"
