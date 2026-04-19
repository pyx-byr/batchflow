import pytest
from batchflow.hook import HookConfig, apply_hooks_start, apply_hooks_end


class TestHookConfig:
    def test_defaults(self):
        h = HookConfig()
        assert h.name == "hooks"
        assert h._on_start == []
        assert h._on_end == []
        assert h._on_item == []
        assert h._on_error == []

    def test_custom_name(self):
        h = HookConfig(name="my_hooks")
        assert h.name == "my_hooks"

    def test_on_start_chaining(self):
        h = HookConfig()
        result = h.on_start(lambda: None)
        assert result is h

    def test_fire_start(self):
        called = []
        h = HookConfig()
        h.on_start(lambda: called.append("start"))
        h.fire_start()
        assert called == ["start"]

    def test_fire_end(self):
        called = []
        h = HookConfig()
        h.on_end(lambda: called.append("end"))
        h.fire_end()
        assert called == ["end"]

    def test_fire_item(self):
        items = []
        h = HookConfig()
        h.on_item(lambda x: items.append(x))
        h.fire_item({"id": 1})
        assert items == [{"id": 1}]

    def test_fire_error(self):
        errors = []
        h = HookConfig()
        h.on_error(lambda item, exc: errors.append((item, str(exc))))
        h.fire_error({"id": 2}, ValueError("oops"))
        assert errors == [({"id": 2}, "oops")]

    def test_multiple_hooks(self):
        called = []
        h = HookConfig()
        h.on_start(lambda: called.append(1))
        h.on_start(lambda: called.append(2))
        h.fire_start()
        assert called == [1, 2]

    def test_apply_hooks_start(self):
        called = []
        h = HookConfig()
        h.on_start(lambda: called.append("s"))
        apply_hooks_start(h)
        assert called == ["s"]

    def test_apply_hooks_end_none(self):
        apply_hooks_end(None)  # should not raise
