from dataclasses import dataclass, field
from typing import Callable, List, Any, Optional


@dataclass
class EventBus:
    name: str = "default"
    _subscribers: dict = field(default_factory=dict, init=False, repr=False)

    def subscribe(self, event: str, handler: Callable) -> "EventBus":
        self._subscribers.setdefault(event, []).append(handler)
        return self

    def unsubscribe(self, event: str, handler: Callable) -> "EventBus":
        if event in self._subscribers:
            self._subscribers[event] = [
                h for h in self._subscribers[event] if h != handler
            ]
        return self

    def publish(self, event: str, payload: Any = None) -> int:
        handlers = self._subscribers.get(event, [])
        for handler in handlers:
            handler(payload)
        return len(handlers)

    def subscribers(self, event: str) -> List[Callable]:
        return list(self._subscribers.get(event, []))

    def clear(self, event: Optional[str] = None) -> "EventBus":
        if event:
            self._subscribers.pop(event, None)
        else:
            self._subscribers.clear()
        return self


def create_event_bus(name: str = "default") -> EventBus:
    return EventBus(name=name)
