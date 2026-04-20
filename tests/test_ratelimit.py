import time
import pytest
from batchflow.ratelimit import RateLimiter, apply_rate_limit


class TestRateLimiter:
    def test_defaults(self):
        r = RateLimiter()
        assert r.name == "default"
        assert r.max_calls == 0
        assert r.period == 1.0

    def test_custom(self):
        r = RateLimiter(name="api", max_calls=5, period=2.0)
        assert r.name == "api"
        assert r.max_calls == 5
        assert r.period == 2.0

    def test_invalid_max_calls(self):
        with pytest.raises(ValueError):
            RateLimiter(max_calls=-1)

    def test_invalid_period(self):
        with pytest.raises(ValueError):
            RateLimiter(period=0)

    def test_not_limited_when_zero(self):
        r = RateLimiter(max_calls=0)
        assert not r.is_limited

    def test_is_limited(self):
        r = RateLimiter(max_calls=3)
        assert r.is_limited

    def test_unlimited_acquire_does_not_block(self):
        r = RateLimiter(max_calls=0)
        start = time.monotonic()
        for _ in range(100):
            r.acquire()
        assert time.monotonic() - start < 0.1

    def test_reset_clears_timestamps(self):
        r = RateLimiter(max_calls=2, period=10.0)
        r.acquire()
        r.acquire()
        assert len(r._timestamps) == 2
        r.reset()
        assert len(r._timestamps) == 0

    def test_apply_rate_limit_calls_fn(self):
        r = RateLimiter(max_calls=10, period=1.0)
        result = apply_rate_limit(r, lambda x: x * 2, 5)
        assert result == 10

    def test_rate_limit_allows_burst(self):
        r = RateLimiter(max_calls=5, period=1.0)
        start = time.monotonic()
        for _ in range(5):
            r.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.2
