import pytest
from batchflow.labeler import LabelerConfig, apply_labeler


class TestLabelerConfig:
    def test_defaults(self):
        lc = LabelerConfig()
        assert lc.name == "labeler"
        assert lc.labeler_count() == 0

    def test_custom_name(self):
        lc = LabelerConfig(name="my_labeler")
        assert lc.name == "my_labeler"

    def test_add_labeler(self):
        lc = LabelerConfig()
        lc.add(lambda item: "positive" if item > 0 else None)
        assert lc.labeler_count() == 1

    def test_add_chaining(self):
        lc = LabelerConfig()
        result = lc.add(lambda item: "a").add(lambda item: "b")
        assert result is lc
        assert lc.labeler_count() == 2

    def test_add_non_callable_raises(self):
        lc = LabelerConfig()
        with pytest.raises(TypeError):
            lc.add("not_a_function")

    def test_label_returns_string_result(self):
        lc = LabelerConfig()
        lc.add(lambda item: "hot" if item > 100 else None)
        assert lc.label(200) == ["hot"]
        assert lc.label(50) == []

    def test_label_with_bool_and_override(self):
        lc = LabelerConfig()
        lc.add(lambda item: item % 2 == 0, label="even")
        assert lc.label(4) == ["even"]
        assert lc.label(3) == []

    def test_label_multiple_matches(self):
        lc = LabelerConfig()
        lc.add(lambda item: "small" if item < 10 else None)
        lc.add(lambda item: "odd" if item % 2 != 0 else None)
        labels = lc.label(7)
        assert "small" in labels
        assert "odd" in labels

    def test_primary_label_returns_first(self):
        lc = LabelerConfig()
        lc.add(lambda item: "first")
        lc.add(lambda item: "second")
        assert lc.primary_label("anything") == "first"

    def test_primary_label_returns_none_when_no_match(self):
        lc = LabelerConfig()
        lc.add(lambda item: None)
        assert lc.primary_label("x") is None

    def test_labeler_ignores_exceptions(self):
        lc = LabelerConfig()
        lc.add(lambda item: 1 / 0)  # will raise ZeroDivisionError
        lc.add(lambda item: "safe")
        assert lc.label("anything") == ["safe"]

    def test_reset_clears_labelers(self):
        lc = LabelerConfig()
        lc.add(lambda item: "x")
        lc.reset()
        assert lc.labeler_count() == 0

    def test_reset_chaining(self):
        lc = LabelerConfig()
        result = lc.reset()
        assert result is lc


class TestApplyLabeler:
    def test_apply_labeler_with_config(self):
        lc = LabelerConfig()
        lc.add(lambda item: "tagged")
        assert apply_labeler("item", lc) == ["tagged"]

    def test_apply_labeler_with_none(self):
        assert apply_labeler("item", None) == []
