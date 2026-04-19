"""Tests for ProgressTracker."""
import time
import pytest
from batchflow.progress import ProgressTracker


class TestProgressTracker:
    def test_initial_state(self):
        tracker = ProgressTracker(total=10, description="Test")
        assert tracker.processed == 0
        assert tracker.failed == 0
        assert tracker.skipped == 0
        assert tracker.total == 10
        assert tracker.percent == 0.0

    def test_increment(self):
        tracker = ProgressTracker(total=4)
        tracker.increment()
        tracker.increment()
        assert tracker.processed == 2
        assert tracker.percent == 50.0

    def test_increment_failed(self):
        tracker = ProgressTracker(total=10)
        tracker.increment_failed()
        assert tracker.failed == 1

    def test_increment_skipped(self):
        tracker = ProgressTracker(total=4)
        tracker.increment_skipped()
        tracker.increment_skipped()
        assert tracker.skipped == 2
        assert tracker.percent == 50.0

    def test_percent_full(self):
        tracker = ProgressTracker(total=5)
        for _ in range(3):
            tracker.increment()
        for _ in range(2):
            tracker.increment_skipped()
        assert tracker.percent == 100.0

    def test_percent_zero_total(self):
        tracker = ProgressTracker(total=0)
        assert tracker.percent == 0.0

    def test_elapsed_increases(self):
        tracker = ProgressTracker(total=10)
        time.sleep(0.05)
        assert tracker.elapsed >= 0.04

    def test_rate(self):
        tracker = ProgressTracker(total=10)
        tracker.increment()
        tracker.increment()
        assert tracker.rate is not None
        assert tracker.rate > 0

    def test_summary_keys(self):
        tracker = ProgressTracker(total=10)
        tracker.increment()
        summary = tracker.summary()
        assert set(summary.keys()) == {"total", "processed", "failed", "skipped", "elapsed", "rate", "percent"}
        assert summary["processed"] == 1
        assert summary["total"] == 10

    def test_str(self):
        tracker = ProgressTracker(total=10, description="Loading")
        tracker.increment()
        result = str(tracker)
        assert "Loading" in result
        assert "10" in result
