import pytest
from batchflow.limiter import LimiterConfig, apply_limiter


class TestLimiterConfig:
    def test_defaults(self):
        lim = LimiterConfig()
        assert lim.name == "limiter"
        assert lim.max_items is None
        assert lim.count == 0

    def test_custom_name(self):
        lim = LimiterConfig(name="my_limiter")
        assert lim.name == "my_limiter"

    def test_custom_max_items(self):
        lim = LimiterConfig(max_items=10)
        assert lim.max_items == 10

    def test_invalid_max_items_zero(self):
        with pytest.raises(ValueError):
            LimiterConfig(max_items=0)

    def test_invalid_max_items_negative(self):
        with pytest.raises(ValueError):
            LimiterConfig(max_items=-5)

    def test_set_max_chaining(self):
        lim = LimiterConfig()
        result = lim.set_max(5)
        assert result is lim
        assert lim.max_items == 5

    def test_set_max_invalid(self):
        lim = LimiterConfig()
        with pytest.raises(ValueError):
            lim.set_max(0)

    def test_is_limited_no_max(self):
        lim = LimiterConfig()
        assert lim.is_limited() is False

    def test_is_limited_below_max(self):
        lim = LimiterConfig(max_items=3)
        lim.increment()
        assert lim.is_limited() is False

    def test_is_limited_at_max(self):
        lim = LimiterConfig(max_items=2)
        lim.increment()
        lim.increment()
        assert lim.is_limited() is True

    def test_reset(self):
        lim = LimiterConfig(max_items=2)
        lim.increment()
        lim.increment()
        lim.reset()
        assert lim.count == 0
        assert lim.is_limited() is False


class TestApplyLimiter:
    def test_none_limiter_passes_item(self):
        result = apply_limiter("hello", None)
        assert result == "hello"

    def test_below_limit_passes_item(self):
        lim = LimiterConfig(max_items=3)
        result = apply_limiter({"id": 1}, lim)
        assert result == {"id": 1}
        assert lim.count == 1

    def test_at_limit_raises_stop_iteration(self):
        lim = LimiterConfig(max_items=1)
        apply_limiter("first", lim)
        with pytest.raises(StopIteration):
            apply_limiter("second", lim)

    def test_increment_called_on_pass(self):
        lim = LimiterConfig(max_items=5)
        for i in range(5):
            apply_limiter(i, lim)
        assert lim.count == 5
        assert lim.is_limited() is True
