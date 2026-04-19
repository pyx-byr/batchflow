"""Throttle configuration for rate-limiting batch pipeline processing."""

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ThrottleConfig:
    """Controls rate-limiting for pipeline item processing."""

    max_per_second: Optional[float] = None
    delay_between_items: float = 0.0
    _last_call_time: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self):
        if self.max_per_second is not None and self.max_per_second <= 0:
            raise ValueError("max_per_second must be a positive number")
        if self.delay_between_items < 0:
            raise ValueError("delay_between_items must be non-negative")
        if self.max_per_second is not None:
            self.delay_between_items = 1.0 / self.max_per_second

    def wait(self) -> None:
        """Block until the next item is allowed to be processed."""
        if self.delay_between_items <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_call_time
        remaining = self.delay_between_items - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_call_time = time.monotonic()

    def reset(self) -> None:
        """Reset the internal timer."""
        self._last_call_time = 0.0


def apply_throttle(throttle: Optional[ThrottleConfig]) -> None:
    """Apply throttle wait if a ThrottleConfig is provided."""
    if throttle is not None:
        throttle.wait()
