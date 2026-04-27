import pytest
from batchflow.window import WindowConfig, apply_window


class TestWindowIntegration:
    def test_tumbling_window_no_overlap(self):
        w = WindowConfig(size=4, step=4)
        result = apply_window(range(8), w)
        assert result == [[0, 1, 2, 3], [4, 5, 6, 7]]

    def test_sliding_window_overlap(self):
        w = WindowConfig(size=3, step=1)
        result = apply_window(range(5), w)
        assert result == [
            [0, 1, 2],
            [1, 2, 3],
            [2, 3, 4],
        ]

    def test_window_larger_than_input_returns_partial(self):
        w = WindowConfig(size=10, step=10)
        result = apply_window([1, 2, 3], w)
        assert result == [[1, 2, 3]]

    def test_empty_input_returns_empty(self):
        w = WindowConfig(size=3, step=3)
        result = apply_window([], w)
        assert result == []

    def test_callback_fires_for_each_window(self):
        fired = []
        w = WindowConfig(size=2, step=2)
        w.on_window(lambda win: fired.append(sum(win)))
        apply_window(range(6), w)
        assert fired == [1, 5, 9]

    def test_step_larger_than_size_skips_items(self):
        w = WindowConfig(size=2, step=4)
        result = apply_window(range(8), w)
        assert result == [[0, 1], [4, 5]]

    def test_reset_allows_reuse(self):
        w = WindowConfig(size=3, step=3)
        first = apply_window([1, 2, 3], w)
        w.reset()
        second = apply_window([4, 5, 6], w)
        assert first == [[1, 2, 3]]
        assert second == [[4, 5, 6]]

    def test_on_window_chaining(self):
        results = []
        w = (
            WindowConfig(size=2, step=2)
            .on_window(lambda win: results.append(list(win)))
        )
        w.add("x")
        w.add("y")
        assert results == [["x", "y"]]
