from typing import Any, Callable, List
from dataclasses import dataclass, field


@dataclass
class SinkConfig:
    name: str = "sink"
    handlers: List[Callable[[Any], None]] = field(default_factory=list)
    stop_on_error: bool = True

    def add(self, fn: Callable[[Any], None]) -> "SinkConfig":
        self.handlers.append(fn)
        return self

    def emit(self, item: Any) -> None:
        for handler in self.handlers:
            try:
                handler(item)
            except Exception as e:
                if self.stop_on_error:
                    raise RuntimeError(
                        f"Sink handler '{handler.__name__}' failed: {e}"
                    ) from e


def collect_to_list(target: list) -> Callable[[Any], None]:
    def _handler(item: Any) -> None:
        target.append(item)
    _handler.__name__ = "collect_to_list"
    return _handler
