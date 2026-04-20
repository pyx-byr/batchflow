import pytest
from batchflow.priority import PriorityConfig, apply_priority


class TestPriorityConfig:
    def test_defaults(self):
        p = PriorityConfig()
        assert p.name == "priority"
        assert p.reverse is False
        assert p.default_priority == 0
        assert p.is_empty()
        assert p.size() == 0

    def test_custom_name(self):
        p = PriorityConfig(name="my_priority")
        assert p.name == "my_priority"

    def test_push_and_pop(self):
        p = PriorityConfig()
        p.set_key(lambda x: x["pri"])
        p.push({"pri": 3, "val": "c"})
        p.push({"pri": 1, "val": "a"})
        p.push({"pri": 2, "val": "b"})
        assert p.size() == 3
        assert p.pop()["val"] == "a"
        assert p.pop()["val"] == "b"
        assert p.pop()["val"] == "c"

    def test_pop_empty_raises(self):
        p = PriorityConfig()
        with pytest.raises(IndexError):
            p.pop()

    def test_peek_empty_raises(self):
        p = PriorityConfig()
        with pytest.raises(IndexError):
            p.peek()

    def test_peek_does_not_remove(self):
        p = PriorityConfig()
        p.set_key(lambda x: x)
        p.push(5)
        p.push(1)
        assert p.peek() == 1
        assert p.size() == 2

    def test_reverse_priority(self):
        p = PriorityConfig(reverse=True)
        p.set_key(lambda x: x)
        p.push(1)
        p.push(3)
        p.push(2)
        assert p.pop() == 3
        assert p.pop() == 2
        assert p.pop() == 1

    def test_default_priority_used_when_no_key(self):
        p = PriorityConfig(default_priority=5)
        p.push("a")
        p.push("b")
        assert p.size() == 2
        result = p.drain()
        assert set(result) == {"a", "b"}

    def test_drain_returns_ordered(self):
        p = PriorityConfig()
        p.set_key(lambda x: x)
        for v in [4, 2, 5, 1, 3]:
            p.push(v)
        assert p.drain() == [1, 2, 3, 4, 5]
        assert p.is_empty()

    def test_reset_clears_queue(self):
        p = PriorityConfig()
        p.set_key(lambda x: x)
        p.push(1)
        p.push(2)
        p.reset()
        assert p.is_empty()
        assert p.size() == 0

    def test_set_key_chaining(self):
        p = PriorityConfig()
        result = p.set_key(lambda x: x)
        assert result is p


class TestApplyPriority:
    def test_apply_priority_orders_items(self):
        p = PriorityConfig()
        p.set_key(lambda x: x["score"])
        items = [{"score": 3}, {"score": 1}, {"score": 2}]
        ordered = apply_priority(items, p)
        assert [i["score"] for i in ordered] == [1, 2, 3]

    def test_apply_priority_empty_list(self):
        p = PriorityConfig()
        assert apply_priority([], p) == []
