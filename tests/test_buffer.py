import pytest
from batchflow.buffer import BufferConfig, apply_buffer


class TestBufferConfig:
    def test_defaults(self):
        buf = BufferConfig()
        assert buf.name == "buffer"
        assert buf.max_size == 100
        assert buf.flush_on_full is True
        assert buf.size == 0

    def test_custom(self):
        buf = BufferConfig(name="my_buf", max_size=10, flush_on_full=False)
        assert buf.name == "my_buf"
        assert buf.max_size == 10
        assert buf.flush_on_full is False

    def test_invalid_max_size_zero(self):
        with pytest.raises(ValueError):
            BufferConfig(max_size=0)

    def test_invalid_max_size_negative(self):
        with pytest.raises(ValueError):
            BufferConfig(max_size=-1)

    def test_add_increments_size(self):
        buf = BufferConfig(max_size=5)
        buf.add("a")
        buf.add("b")
        assert buf.size == 2

    def test_flush_clears_buffer(self):
        buf = BufferConfig(max_size=5)
        buf.add(1)
        buf.add(2)
        items = buf.flush()
        assert items == [1, 2]
        assert buf.size == 0

    def test_flush_on_full_triggers_handler(self):
        received = []
        buf = BufferConfig(max_size=3)
        buf.on_flush(lambda items: received.extend(items))
        buf.add("x")
        buf.add("y")
        assert received == []
        buf.add("z")  # triggers flush
        assert received == ["x", "y", "z"]
        assert buf.size == 0

    def test_no_auto_flush_when_disabled(self):
        flushed = []
        buf = BufferConfig(max_size=2, flush_on_full=False)
        buf.on_flush(lambda items: flushed.extend(items))
        buf.add(1)
        buf.add(2)
        assert flushed == []
        assert buf.size == 2

    def test_on_flush_chaining(self):
        buf = BufferConfig()
        result = buf.on_flush(lambda items: None)
        assert result is buf

    def test_is_full(self):
        buf = BufferConfig(max_size=2, flush_on_full=False)
        buf.add(1)
        assert not buf.is_full
        buf.add(2)
        assert buf.is_full

    def test_peek_does_not_clear(self):
        buf = BufferConfig(max_size=10)
        buf.add("a")
        snapshot = buf.peek()
        assert snapshot == ["a"]
        assert buf.size == 1

    def test_clear_discards_without_handlers(self):
        flushed = []
        buf = BufferConfig(max_size=10)
        buf.on_flush(lambda items: flushed.extend(items))
        buf.add(1)
        buf.clear()
        assert buf.size == 0
        assert flushed == []

    def test_multiple_flush_handlers(self):
        results_a, results_b = [], []
        buf = BufferConfig(max_size=2)
        buf.on_flush(lambda items: results_a.extend(items))
        buf.on_flush(lambda items: results_b.extend(items))
        buf.add("p")
        buf.add("q")
        assert results_a == ["p", "q"]
        assert results_b == ["p", "q"]


class TestApplyBuffer:
    def test_none_buffer_returns_false(self):
        assert apply_buffer(None, "item") is False

    def test_returns_true_on_flush(self):
        buf = BufferConfig(max_size=1)
        result = apply_buffer(buf, "only")
        assert result is True

    def test_returns_false_before_flush(self):
        buf = BufferConfig(max_size=5)
        assert apply_buffer(buf, "a") is False
        assert apply_buffer(buf, "b") is False
