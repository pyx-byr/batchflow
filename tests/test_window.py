import pytest
from batchflow.window import WindowConfig, apply_window


class TestWindowConfig:
    def test_defaults(self):
        w = WindowConfig()
        assert w.name == "window"
        assert w.size == 10
        assert w.step == 1
        assert w.buffer_size == 0

    def test_custom(self):
        w = WindowConfig(name="my_window", size=5, step=3)
        assert w.name == "my_window"
        assert w.size == 5
        assert w.step == 3

    def test_invalid_size_zero(self):
        with pytest.raises(ValueError, match="size"):
            WindowConfig(size=0)

    def test_invalid_size_negative(self):
        with pytest.raises(ValueError, match="size"):
            WindowConfig(size=-1)

    def test_invalid_step_zero(self):
        with pytest.raises(ValueError, match="step"):
            WindowConfig(size=5, step=0)

    def test_invalid_step_negative(self):
        with pytest.raises(ValueError, match="step"):
            WindowConfig(size=5, step=-2)

    def test_add_does_not_emit_until_full(self):
        w = WindowConfig(size=3, step=3)
        assert w.add(1) == []
        assert w.add(2) == []
        assert w.buffer_size == 2

    def test_add_emits_when_full(self):
        w = WindowConfig(size=3, step=3)
        w.add(1)
        w.add(2)
        result = w.add(3)
        assert result == [[1, 2, 3]]
        assert w.buffer_size == 0

    def test_sliding_window(self):
        w = WindowConfig(size=3, step=1)
        results = []
        for i in range(5):
            results.extend(w.add(i))
        assert results == [[0, 1, 2], [1, 2, 3], [2, 3, 4]]

    def test_on_window_callback(self):
        fired = []
        w = WindowConfig(size=2, step=2)
        w.on_window(lambda win: fired.append(list(win)))
        w.add("a")
        w.add("b")
        assert fired == [["a", "b"]]

    def test_flush_returns_partial(self):
        w = WindowConfig(size=4, step=4)
        w.add(1)
        w.add(2)
        partial = w.flush()
        assert partial == [1, 2]
        assert w.buffer_size == 0

    def test_flush_returns_none_when_empty(self):
        w = WindowConfig(size=4, step=4)
        assert w.flush() is None

    def test_reset_clears_buffer(self):
        w = WindowConfig(size=5, step=5)
        w.add(1)
        w.add(2)
        w.reset()
        assert w.buffer_size == 0

    def test_apply_window_tumbling(self):
        w = WindowConfig(size=3, step=3)
        result = apply_window(range(9), w)
        assert result == [[0, 1, 2], [3, 4, 5], [6, 7, 8]]

    def test_apply_window_with_partial(self):
        w = WindowConfig(size=3, step=3)
        result = apply_window(range(7), w)
        assert result == [[0, 1, 2], [3, 4, 5], [6]]
