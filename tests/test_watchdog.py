"""Unit tests for WatchdogConfig."""

import time
import pytest
from batchflow.watchdog import WatchdogConfig, apply_watchdog


class TestWatchdogConfig:
    def test_defaults(self):
        wd = WatchdogConfig()
        assert wd.name == "watchdog"
        assert wd.timeout_seconds == 30.0
        assert wd.on_stall is None
        assert wd.enabled is True

    def test_custom_name(self):
        wd = WatchdogConfig(name="my_watchdog")
        assert wd.name == "my_watchdog"

    def test_invalid_timeout_zero(self):
        with pytest.raises(ValueError):
            WatchdogConfig(timeout_seconds=0)

    def test_invalid_timeout_negative(self):
        with pytest.raises(ValueError):
            WatchdogConfig(timeout_seconds=-5.0)

    def test_no_stall_before_timeout(self):
        wd = WatchdogConfig(timeout_seconds=10.0)
        assert wd.check() is False

    def test_stall_detected_after_timeout(self):
        wd = WatchdogConfig(timeout_seconds=0.01)
        time.sleep(0.05)
        assert wd.check() is True

    def test_on_stall_callback_fired(self):
        fired = []
        wd = WatchdogConfig(timeout_seconds=0.01, on_stall=lambda elapsed: fired.append(elapsed))
        time.sleep(0.05)
        wd.check()
        assert len(fired) == 1
        assert fired[0] >= 0.01

    def test_stall_callback_fires_once(self):
        fired = []
        wd = WatchdogConfig(timeout_seconds=0.01, on_stall=lambda e: fired.append(e))
        time.sleep(0.05)
        wd.check()
        wd.check()
        assert len(fired) == 1

    def test_heartbeat_resets_stall(self):
        wd = WatchdogConfig(timeout_seconds=0.01)
        time.sleep(0.05)
        wd.heartbeat()
        assert wd.check() is False

    def test_reset(self):
        wd = WatchdogConfig(timeout_seconds=0.01)
        time.sleep(0.05)
        wd.reset()
        assert wd.check() is False

    def test_disabled_never_stalls(self):
        wd = WatchdogConfig(timeout_seconds=0.01, enabled=False)
        time.sleep(0.05)
        assert wd.check() is False

    def test_seconds_since_heartbeat(self):
        wd = WatchdogConfig()
        time.sleep(0.05)
        assert wd.seconds_since_heartbeat >= 0.05

    def test_heartbeat_chaining(self):
        wd = WatchdogConfig()
        result = wd.heartbeat()
        assert result is wd


class TestApplyWatchdog:
    def test_returns_item_unchanged(self):
        wd = WatchdogConfig()
        item = {"key": "value"}
        result = apply_watchdog(wd, item)
        assert result is item

    def test_none_watchdog_passthrough(self):
        item = {"key": "value"}
        result = apply_watchdog(None, item)
        assert result is item

    def test_apply_sends_heartbeat(self):
        wd = WatchdogConfig(timeout_seconds=0.01)
        time.sleep(0.05)
        apply_watchdog(wd, "item")
        assert wd.check() is False
