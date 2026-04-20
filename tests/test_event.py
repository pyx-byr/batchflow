import pytest
from batchflow.event import EventBus, create_event_bus


class TestEventBus:
    def test_defaults(self):
        bus = EventBus()
        assert bus.name == "default"
        assert bus.subscribers("any") == []

    def test_custom_name(self):
        bus = EventBus(name="pipeline")
        assert bus.name == "pipeline"

    def test_subscribe_and_publish(self):
        bus = EventBus()
        results = []
        bus.subscribe("item.processed", lambda p: results.append(p))
        count = bus.publish("item.processed", {"id": 1})
        assert count == 1
        assert results == [{"id": 1}]

    def test_multiple_subscribers(self):
        bus = EventBus()
        calls = []
        bus.subscribe("start", lambda p: calls.append("a"))
        bus.subscribe("start", lambda p: calls.append("b"))
        bus.publish("start")
        assert calls == ["a", "b"]

    def test_publish_no_subscribers_returns_zero(self):
        bus = EventBus()
        count = bus.publish("nonexistent")
        assert count == 0

    def test_unsubscribe(self):
        bus = EventBus()
        handler = lambda p: None
        bus.subscribe("ev", handler)
        bus.unsubscribe("ev", handler)
        assert bus.subscribers("ev") == []

    def test_subscribe_chaining(self):
        bus = EventBus()
        result = bus.subscribe("x", lambda p: None)
        assert result is bus

    def test_clear_single_event(self):
        bus = EventBus()
        bus.subscribe("a", lambda p: None)
        bus.subscribe("b", lambda p: None)
        bus.clear("a")
        assert bus.subscribers("a") == []
        assert len(bus.subscribers("b")) == 1

    def test_clear_all(self):
        bus = EventBus()
        bus.subscribe("a", lambda p: None)
        bus.subscribe("b", lambda p: None)
        bus.clear()
        assert bus.subscribers("a") == []
        assert bus.subscribers("b") == []

    def test_create_event_bus_factory(self):
        bus = create_event_bus("test")
        assert isinstance(bus, EventBus)
        assert bus.name == "test"
