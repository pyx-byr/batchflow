import pytest
from batchflow.sink import SinkConfig, collect_to_list


class TestSinkConfig:
    def test_defaults(self):
        sc = SinkConfig()
        assert sc.name == "sink"
        assert sc.handlers == []
        assert sc.stop_on_error is True

    def test_add_handler(self):
        sc = SinkConfig()
        sc.add(lambda x: None)
        assert len(sc.handlers) == 1

    def test_add_chaining(self):
        sc = SinkConfig()
        result = sc.add(lambda x: None).add(lambda x: None)
        assert result is sc
        assert len(sc.handlers) == 2

    def test_emit_calls_handlers(self):
        results = []
        sc = SinkConfig()
        sc.add(results.append)
        sc.emit(42)
        assert results == [42]

    def test_emit_multiple_handlers(self):
        a, b = [], []
        sc = SinkConfig()
        sc.add(a.append).add(b.append)
        sc.emit("x")
        assert a == ["x"]
        assert b == ["x"]

    def test_emit_error_raises(self):
        def bad(item):
            raise ValueError("oops")
        sc = SinkConfig(stop_on_error=True)
        sc.add(bad)
        with pytest.raises(RuntimeError):
            sc.emit(1)

    def test_emit_error_ignored(self):
        results = []
        def bad(item):
            raise ValueError("oops")
        sc = SinkConfig(stop_on_error=False)
        sc.add(bad)
        sc.add(results.append)
        sc.emit(99)
        assert results == [99]


class TestCollectToList:
    def test_appends_to_list(self):
        target = []
        handler = collect_to_list(target)
        handler(1)
        handler(2)
        assert target == [1, 2]
