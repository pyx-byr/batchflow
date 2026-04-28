"""Integration tests for WatchdogConfig in realistic pipeline-like scenarios."""

import time
import pytest
from batchflow.watchdog import WatchdogConfig, apply_watchdog


class TestWatchdogIntegration:
    def test_heartbeat_per_item_prevents_stall(self):
        stalls = []
        wd = WatchdogConfig(timeout_seconds=1.0, on_stall=lambda e: stalls.append(e))
        items = list(range(5))
        for item in items:
            apply_watchdog(wd, item)
        assert stalls == []

    def test_stall_detected_between_slow_items(self):
        stalls = []
        wd = WatchdogConfig(timeout_seconds=0.05, on_stall=lambda e: stalls.append(e))
        apply_watchdog(wd, "item_1")
        time.sleep(0.1)
        wd.check()
        assert len(stalls) == 1

    def test_multiple_stall_windows_each_fire_once(self):
        fired = []
        wd = WatchdogConfig(timeout_seconds=0.05, on_stall=lambda e: fired.append(e))
        # First stall window
        time.sleep(0.1)
        wd.check()
        wd.check()  # should not re-fire
        assert len(fired) == 1
        # Reset via heartbeat, then stall again
        wd.heartbeat()
        time.sleep(0.1)
        wd.check()
        assert len(fired) == 2

    def test_disabled_watchdog_never_fires(self):
        fired = []
        wd = WatchdogConfig(timeout_seconds=0.01, enabled=False, on_stall=lambda e: fired.append(e))
        time.sleep(0.05)
        wd.check()
        assert fired == []

    def test_apply_watchdog_with_none_is_safe(self):
        items = [1, 2, 3]
        results = [apply_watchdog(None, item) for item in items]
        assert results == items

    def test_reset_clears_stall_flag(self):
        fired = []
        wd = WatchdogConfig(timeout_seconds=0.01, on_stall=lambda e: fired.append(e))
        time.sleep(0.05)
        wd.check()  # fires once
        assert len(fired) == 1
        wd.reset()
        time.sleep(0.05)
        wd.check()  # should fire again after reset
        assert len(fired) == 2
