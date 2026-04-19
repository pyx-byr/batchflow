from dataclasses import dataclass, field
from typing import Callable, List, Any


@dataclass
class HookConfig:
    """Configuration for pipeline lifecycle hooks."""
    name: str = "hooks"
    _on_start: List[Callable] = field(default_factory=list, init=False)
    _on_end: List[Callable] = field(default_factory=list, init=False)
    _on_item: List[Callable] = field(default_factory=list, init=False)
    _on_error: List[Callable] = field(default_factory=list, init=False)

    def on_start(self, fn: Callable) -> "HookConfig":
        self._on_start.append(fn)
        return self

    def on_end(self, fn: Callable) -> "HookConfig":
        self._on_end.append(fn)
        return self

    def on_item(self, fn: Callable) -> "HookConfig":
        self._on_item.append(fn)
        return self

    def on_error(self, fn: Callable) -> "HookConfig":
        self._on_error.append(fn)
        return self

    def fire_start(self) -> None:
        for fn in self._on_start:
            fn()

    def fire_end(self) -> None:
        for fn in self._on_end:
            fn()

    def fire_item(self, item: Any) -> None:
        for fn in self._on_item:
            fn(item)

    def fire_error(self, item: Any, exc: Exception) -> None:
        for fn in self._on_error:
            fn(item, exc)


def apply_hooks_start(hooks: HookConfig) -> None:
    if hooks:
        hooks.fire_start()


def apply_hooks_end(hooks: HookConfig) -> None:
    if hooks:
        hooks.fire_end()
