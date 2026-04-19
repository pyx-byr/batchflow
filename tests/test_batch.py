"""Tests for batchflow.batch module."""
import pytest
from batchflow.batch import BatchConfig, iter_batches, split_batches


class TestBatchConfig:
    def test_defaults(self):
        cfg = BatchConfig()
        assert cfg.size == 10
        assert cfg.drop_last is False
        assert cfg.label == "batch"

    def test_custom(self):
        cfg = BatchConfig(size=3, drop_last=True, label="chunk")
        assert cfg.size == 3
        assert cfg.drop_last is True
        assert cfg.label == "chunk"

    def test_invalid_size(self):
        with pytest.raises(ValueError, match=">= 1"):
            BatchConfig(size=0)

    def test_invalid_size_negative(self):
        with pytest.raises(ValueError):
            BatchConfig(size=-5)


class TestIterBatches:
    def _cfg(self, size=3, drop_last=False):
        return BatchConfig(size=size, drop_last=drop_last)

    def test_exact_multiple(self):
        result = split_batches(range(6), self._cfg(3))
        assert result == [[0, 1, 2], [3, 4, 5]]

    def test_remainder_kept(self):
        result = split_batches(range(7), self._cfg(3))
        assert result == [[0, 1, 2], [3, 4, 5], [6]]

    def test_remainder_dropped(self):
        result = split_batches(range(7), self._cfg(3, drop_last=True))
        assert result == [[0, 1, 2], [3, 4, 5]]

    def test_empty_iterable(self):
        assert split_batches([], self._cfg(3)) == []

    def test_empty_drop_last(self):
        assert split_batches([], self._cfg(3, drop_last=True)) == []

    def test_single_item(self):
        assert split_batches([42], self._cfg(3)) == [[42]]

    def test_single_item_drop_last(self):
        assert split_batches([42], self._cfg(3, drop_last=True)) == []

    def test_iter_batches_is_lazy(self):
        """iter_batches should return an iterator, not a list."""
        gen = iter_batches(range(10), self._cfg(3))
        import types
        assert isinstance(gen, types.GeneratorType)

    def test_large_batch_size(self):
        result = split_batches(range(5), self._cfg(100))
        assert result == [list(range(5))]
