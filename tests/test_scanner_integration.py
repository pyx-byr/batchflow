import pytest
from batchflow.scanner import ScannerConfig


class TestScannerIntegration:
    def test_running_average(self):
        s = ScannerConfig()
        s.add("sum", lambda acc, x: acc + x, initial=0)
        s.add("count", lambda acc, x: acc + 1, initial=0)
        items = [10, 20, 30, 40]
        for item in items:
            s.scan(item)
        total = s.get_state("sum")
        count = s.get_state("count")
        assert total / count == 25.0

    def test_track_min_and_max(self):
        s = ScannerConfig()
        s.add("min", lambda acc, x: x if acc is None or x < acc else acc, initial=None)
        s.add("max", lambda acc, x: x if acc is None or x > acc else acc, initial=None)
        for v in [5, 2, 8, 1, 9, 3]:
            s.scan(v)
        assert s.get_state("min") == 1
        assert s.get_state("max") == 9

    def test_collect_unique_labels(self):
        s = ScannerConfig()
        s.add(
            "labels",
            lambda acc, x: acc | {x.get("label")} if x.get("label") else acc,
            initial=set(),
        )
        items = [
            {"label": "a"},
            {"label": "b"},
            {"label": "a"},
            {"label": "c"},
        ]
        for item in items:
            s.scan(item)
        assert s.get_state("labels") == {"a", "b", "c"}

    def test_reset_and_reuse(self):
        s = ScannerConfig()
        s.add("total", lambda acc, x: acc + x, initial=0)
        for v in [1, 2, 3]:
            s.scan(v)
        assert s.get_state("total") == 6
        s.reset()
        for v in [10, 20]:
            s.scan(v)
        assert s.get_state("total") == 30

    def test_empty_input_preserves_initial_state(self):
        s = ScannerConfig()
        s.add("total", lambda acc, x: acc + x, initial=0)
        assert s.get_state("total") == 0
        assert s.scanner_count == 1
