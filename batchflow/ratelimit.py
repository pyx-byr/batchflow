from dataclasses import dataclass, field
from collections import deque
import time


@dataclass
class RateLimiter:
    """Token-bucket style rate limiter for pipeline processing."""
    name: str = "default"
    max_calls: int = 0  # 0 = unlimited
    period: float = 1.0  # seconds
    _timestamps: deque = field(default_factory=deque, init=False, repr=False)

    def __post_init__(self):
        if self.max_calls < 0:
            raise ValueError("max_calls must be >= 0")
        if self.period <= 0:
            raise ValueError("period must be > 0")

    @property
    def is_limited(self) -> bool:
        return self.max_calls > 0

    def acquire(self) -> None:
        """Block until a call is allowed under the rate limit."""
        if not self.is_limited:
            return
        now = time.monotonic()
        cutoff = now - self.period
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
        if len(self._timestamps) >= self.max_calls:
            oldest = self._timestamps[0]
            sleep_for = self.period - (now - oldest)
            if sleep_for > 0:
                time.sleep(sleep_for)
            now = time.monotonic()
            cutoff = now - self.period
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
        self._timestamps.append(time.monotonic())

    def reset(self) -> None:
        self._timestamps.clear()


def apply_rate_limit(limiter: RateLimiter, fn, *args, **kwargs):
    """Acquire rate limit slot then call fn."""
    limiter.acquire()
    return fn(*args, **kwargs)
