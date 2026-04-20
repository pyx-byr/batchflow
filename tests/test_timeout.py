import time
import pytest
from batchflow.timeout import TimeoutConfig, apply_timeout


class TestTimeoutConfig:
    def test_defaults(self):
        t = TimeoutConfig()
        assert t.seconds == 0.0
        assert t.name == "timeout"

    def test_custom(self):
        t = TimeoutConfig(seconds=5.0, name="my_timeout")
        assert t.seconds == 5.0
        assert t.name == "my_timeout"

    def test_negative_seconds_raises(self):
        with pytest.raises(ValueError):
            TimeoutConfig(seconds=-1)

    def test_enabled_false_when_zero(self):
        assert TimeoutConfig(seconds=0).enabled is False

    def test_enabled_true_when_positive(self):
        assert TimeoutConfig(seconds=1).enabled is True

    def test_run_without_timeout(self):
        t = TimeoutConfig(seconds=0)
        result = t.run(lambda x: x * 2, 5)
        assert result == 10

    def test_run_completes_within_timeout(self):
        t = TimeoutConfig(seconds=2.0)
        result = t.run(lambda: 42)
        assert result == 42

    def test_run_raises_on_timeout(self):
        t = TimeoutConfig(seconds=0.1)
        with pytest.raises(TimeoutError):
            t.run(time.sleep, 5)


class TestApplyTimeout:
    def test_apply_none_config(self):
        result = apply_timeout(None, lambda: 99)
        assert result == 99

    def test_apply_disabled_config(self):
        t = TimeoutConfig(seconds=0)
        result = apply_timeout(t, lambda: 7)
        assert result == 7

    def test_apply_with_timeout_success(self):
        t = TimeoutConfig(seconds=2.0)
        result = apply_timeout(t, lambda x: x + 1, 10)
        assert result == 11

    def test_apply_with_timeout_exceeded(self):
        t = TimeoutConfig(seconds=0.1)
        with pytest.raises(TimeoutError):
            apply_timeout(t, time.sleep, 5)
