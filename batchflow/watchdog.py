"""Watchdog module for monitoring pipeline health and triggering alerts on stalls."""

from dataclasses import dataclass, field
from typing import Callable, Optional
import time


@dataclass
class WatchdogConfig:
    """Monitors pipeline activity and fires a callback if no progress is made within a timeout."""

    name: str = "watchdog"
    timeout_seconds: float = 30.0
    on_stall: Optional[Callable[[float], None]] = None
    enabled: bool = True

    def __post_init__(self):
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._last_heartbeat: float = time.monotonic()
        self._stall_fired: bool = False

    def heartbeat(self) -> "WatchdogConfig":
        """Record that the pipeline is still making progress."""
        self._last_heartbeat = time.monotonic()
        self._stall_fired = False
        return self

    def check(self) -> bool:
        """Check if a stall has occurred. Fires on_stall callback if so. Returns True if stalled."""
        if not self.enabled:
            return False
        elapsed = time.monotonic() - self._last_heartbeat
        if elapsed >= self.timeout_seconds and not self._stall_fired:
            self._stall_fired = True
            if self.on_stall:
                self.on_stall(elapsed)
            return True
        return False

    def reset(self) -> "WatchdogConfig":
        """Reset the watchdog state."""
        self._last_heartbeat = time.monotonic()
        self._stall_fired = False
        return self

    @property
    def seconds_since_heartbeat(self) -> float:
        """Return seconds elapsed since the last heartbeat."""
        return time.monotonic() - self._last_heartbeat


def apply_watchdog(watchdog: Optional[WatchdogConfig], item: object) -> object:
    """Send a heartbeat for the given item and return it unchanged."""
    if watchdog is not None:
        watchdog.heartbeat()
    return item
