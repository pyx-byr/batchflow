"""Tests for batchflow.filter module."""
import pytest
from batchflow.filter import FilterConfig, apply_filters


class TestFilterConfig:
    def test_default_attributes(self):
        pred = lambda x: x > 0
        fc = FilterConfig(predicate=pred)
        assert fc.predicate is pred
        assert fc.log_filtered is False
        assert fc.label == "filter"

    def test_custom_label(self):
        fc = FilterConfig(predicate=lambda x: True, label="positive_only")
        assert fc.label == "positive_only"

    def test_should_process_true(self):
        fc = FilterConfig(predicate=lambda x: x % 2 == 0)
        assert fc.should_process(4) is True

    def test_should_process_false(self):
        fc = FilterConfig(predicate=lambda x: x % 2 == 0)
        assert fc.should_process(3) is False

    def test_should_process_predicate_raises(self):
        def bad_pred(x):
            raise RuntimeError("boom")

        fc = FilterConfig(predicate=bad_pred, label="bad")
        with pytest.raises(ValueError, match="bad"):
            fc.should_process(1)


class TestApplyFilters:
    def test_no_filters_returns_all(self):
        items = [1, 2, 3]
        assert apply_filters(items, []) == items

    def test_single_filter(self):
        fc = FilterConfig(predicate=lambda x: x > 2)
        assert apply_filters([1, 2, 3, 4], [fc]) == [3, 4]

    def test_multiple_filters_and_logic(self):
        even = FilterConfig(predicate=lambda x: x % 2 == 0, label="even")
        gt2 = FilterConfig(predicate=lambda x: x > 2, label="gt2")
        assert apply_filters([1, 2, 3, 4, 6], [even, gt2]) == [4, 6]

    def test_all_filtered_out(self):
        fc = FilterConfig(predicate=lambda x: x > 100)
        assert apply_filters([1, 2, 3], [fc]) == []

    def test_empty_items(self):
        fc = FilterConfig(predicate=lambda x: True)
        assert apply_filters([], [fc]) == []
