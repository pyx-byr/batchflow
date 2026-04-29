import pytest
from batchflow.scanner import ScannerConfig, apply_scanner


class TestScannerConfig:
    def test_defaults(self):
        s = ScannerConfig()
        assert s.name == "scanner"
        assert s.scanner_count == 0
        assert s.state_snapshot == {}

    def test_custom_name(self):
        s = ScannerConfig(name="my_scanner")
        assert s.name == "my_scanner"

    def test_add_scanner(self):
        s = ScannerConfig()
        s.add("total", lambda acc, x: (acc or 0) + x, initial=0)
        assert s.scanner_count == 1

    def test_add_chaining(self):
        s = ScannerConfig()
        result = s.add("count", lambda acc, x: (acc or 0) + 1, initial=0)
        assert result is s

    def test_add_non_callable_raises(self):
        s = ScannerConfig()
        with pytest.raises(TypeError):
            s.add("bad", "not_a_function")

    def test_scan_accumulates_sum(self):
        s = ScannerConfig()
        s.add("total", lambda acc, x: acc + x, initial=0)
        s.scan(5)
        s.scan(10)
        s.scan(3)
        assert s.get_state("total") == 18

    def test_scan_accumulates_count(self):
        s = ScannerConfig()
        s.add("count", lambda acc, x: acc + 1, initial=0)
        for _ in range(7):
            s.scan("item")
        assert s.get_state("count") == 7

    def test_scan_returns_item_unchanged(self):
        s = ScannerConfig()
        s.add("total", lambda acc, x: acc + x, initial=0)
        item = {"value": 42}
        result = s.scan(item)
        assert result is item

    def test_get_state_default(self):
        s = ScannerConfig()
        assert s.get_state("missing", default="fallback") == "fallback"

    def test_reset_clears_state(self):
        s = ScannerConfig()
        s.add("total", lambda acc, x: acc + x, initial=0)
        s.scan(10)
        s.scan(20)
        s.reset()
        assert s.get_state("total") == 0

    def test_state_snapshot(self):
        s = ScannerConfig()
        s.add("total", lambda acc, x: acc + x, initial=0)
        s.add("count", lambda acc, x: acc + 1, initial=0)
        s.scan(5)
        snapshot = s.state_snapshot
        assert snapshot == {"total": 5, "count": 1}

    def test_multiple_scanners(self):
        s = ScannerConfig()
        s.add("sum", lambda acc, x: acc + x, initial=0)
        s.add("max", lambda acc, x: x if acc is None or x > acc else acc, initial=None)
        for v in [3, 7, 1, 9, 2]:
            s.scan(v)
        assert s.get_state("sum") == 22
        assert s.get_state("max") == 9


class TestApplyScanner:
    def test_none_scanner_returns_item(self):
        item = {"x": 1}
        assert apply_scanner(item, None) is item

    def test_applies_scanner(self):
        s = ScannerConfig()
        s.add("count", lambda acc, x: acc + 1, initial=0)
        apply_scanner("a", s)
        apply_scanner("b", s)
        assert s.get_state("count") == 2
