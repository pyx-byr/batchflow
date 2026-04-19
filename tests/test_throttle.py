"""Tests for ThrottleConfig and apply_throttle."""

import time
import pytest
from batchflow.throttle import ThrottleConfig, apply_throttle


class TestThrottleConfig:
    def test_defaults(self):
        t = ThrottleConfig()
        assert t.max_per_second is None
        assert t.delay_between_items == 0.0

    def test_max_per_second_sets_delay(self):
        t = ThrottleConfig(max_per_second=4.0)
        assert t.delay_between_items == pytest.approx(0.25)

    def test_invalid_max_per_second_zero(self):
        with pytest.raises(ValueError, match="max_per_second"):
            ThrottleConfig(max_per_second=0)

    def test_invalid_max_per_second_negative(self):
        with pytest.raises(ValueError, match="max_per_second"):
            ThrottleConfig(max_per_second=-1)

    def test_invalid_delay_negative(self):
        with pytest.raises(ValueError, match="delay_between_items"):
            ThrottleConfig(delay_between_items=-0.1)

    def test_custom_delay(self):
        t = ThrottleConfig(delay_between_items=0.05)
        assert t.delay_between_items == 0.05

    def test_reset(self):
        t = ThrottleConfig(delay_between_items=0.05)
        t._last_call_time = time.monotonic()
        t.reset()
        assert t._last_call_time == 0.0

    def test_wait_no_delay(self):
        t = ThrottleConfig()
        start = time.monotonic()
        t.wait()
        assert time.monotonic() - start < 0.01

    def test_wait_enforces_delay(self):
        t = ThrottleConfig(delay_between_items=0.05)
        t.wait()  # first call, sets timer
        start = time.monotonic()
        t.wait()  # second call should wait
        elapsed = time.monotonic() - start
        assert elapsed >= 0.04


class TestApplyThrottle:
    def test_none_throttle_does_nothing(self):
        start = time.monotonic()
        apply_throttle(None)
        assert time.monotonic() - start < 0.01

    def test_applies_throttle(self):
        t = ThrottleConfig(delay_between_items=0.05)
        t.wait()
        start = time.monotonic()
        apply_throttle(t)
        assert time.monotonic() - start >= 0.04
