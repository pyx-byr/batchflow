import time
import pytest
from batchflow.metrics import MetricsCollector


class TestMetricsCollector:
    def test_defaults(self):
        m = MetricsCollector()
        assert m.name == "default"
        assert m.sample_count == 0
        assert m.total_time == 0.0
        assert m.avg_time == 0.0

    def test_custom_name(self):
        m = MetricsCollector(name="pipeline-a")
        assert m.name == "pipeline-a"

    def test_increment(self):
        m = MetricsCollector()
        m.increment("processed")
        m.increment("processed", 4)
        assert m.get("processed") == 5

    def test_get_default(self):
        m = MetricsCollector()
        assert m.get("missing", 99) == 99

    def test_timer(self):
        m = MetricsCollector()
        m.start_timer()
        time.sleep(0.01)
        elapsed = m.stop_timer()
        assert elapsed >= 0.01
        assert m.sample_count == 1
        assert m.total_time >= 0.01

    def test_avg_time(self):
        m = MetricsCollector()
        m._timings = [0.1, 0.3]
        assert abs(m.avg_time - 0.2) < 1e-9

    def test_stop_timer_without_start(self):
        m = MetricsCollector()
        assert m.stop_timer() == 0.0

    def test_summary(self):
        m = MetricsCollector(name="test")
        m.increment("ok", 3)
        m._timings = [0.5]
        s = m.summary()
        assert s["name"] == "test"
        assert s["counts"] == {"ok": 3}
        assert s["samples"] == 1
        assert s["total_time"] == 0.5

    def test_reset(self):
        m = MetricsCollector()
        m.increment("x", 5)
        m._timings = [1.0, 2.0]
        m.reset()
        assert m.get("x") == 0
        assert m.sample_count == 0
        assert m.total_time == 0.0
