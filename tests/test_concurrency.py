import pytest
from batchflow.concurrency import ConcurrencyConfig, apply_concurrently


class TestConcurrencyConfig:
    def test_defaults(self):
        cfg = ConcurrencyConfig()
        assert cfg.max_workers == 1
        assert cfg.timeout is None

    def test_custom(self):
        cfg = ConcurrencyConfig(max_workers=4, timeout=5.0)
        assert cfg.max_workers == 4
        assert cfg.timeout == 5.0

    def test_invalid_max_workers_zero(self):
        with pytest.raises(ValueError, match="max_workers"):
            ConcurrencyConfig(max_workers=0)

    def test_invalid_max_workers_negative(self):
        with pytest.raises(ValueError, match="max_workers"):
            ConcurrencyConfig(max_workers=-1)

    def test_invalid_timeout(self):
        with pytest.raises(ValueError, match="timeout"):
            ConcurrencyConfig(max_workers=2, timeout=-1.0)

    def test_is_concurrent_false(self):
        assert ConcurrencyConfig(max_workers=1).is_concurrent is False

    def test_is_concurrent_true(self):
        assert ConcurrencyConfig(max_workers=2).is_concurrent is True


class TestApplyConcurrently:
    def test_sequential_success(self):
        cfg = ConcurrencyConfig(max_workers=1)
        results = apply_concurrently(lambda x: x * 2, [1, 2, 3], cfg)
        assert len(results) == 3
        for item, result, exc in results:
            assert result == item * 2
            assert exc is None

    def test_sequential_error(self):
        def boom(x):
            if x == 2:
                raise ValueError("bad")
            return x

        cfg = ConcurrencyConfig(max_workers=1)
        results = apply_concurrently(boom, [1, 2, 3], cfg)
        errors = [(item, exc) for item, _, exc in results if exc is not None]
        assert len(errors) == 1
        assert errors[0][0] == 2

    def test_concurrent_success(self):
        cfg = ConcurrencyConfig(max_workers=4)
        results = apply_concurrently(lambda x: x + 10, range(10), cfg)
        assert len(results) == 10
        values = {item: result for item, result, exc in results if exc is None}
        for i in range(10):
            assert values[i] == i + 10

    def test_concurrent_partial_failure(self):
        def maybe_fail(x):
            if x % 2 == 0:
                raise RuntimeError("even")
            return x

        cfg = ConcurrencyConfig(max_workers=3)
        results = apply_concurrently(maybe_fail, range(6), cfg)
        assert len(results) == 6
        failures = [r for r in results if r[2] is not None]
        assert len(failures) == 3
