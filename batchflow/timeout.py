from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Callable, Any, Optional


@dataclass
class TimeoutConfig:
    """Configuration for per-item processing timeouts."""
    seconds: float = 0.0
    name: str = "timeout"

    def __post_init__(self):
        if self.seconds < 0:
            raise ValueError("seconds must be >= 0")

    @property
    def enabled(self) -> bool:
        return self.seconds > 0

    def run(self, fn: Callable, *args, **kwargs) -> Any:
        """Run fn with a timeout. Raises TimeoutError if exceeded."""
        if not self.enabled:
            return fn(*args, **kwargs)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(fn, *args, **kwargs)
            try:
                return future.result(timeout=self.seconds)
            except FuturesTimeoutError:
                raise TimeoutError(
                    f"[{self.name}] Item processing exceeded {self.seconds}s timeout"
                )


def apply_timeout(config: Optional[TimeoutConfig], fn: Callable, *args, **kwargs) -> Any:
    """Apply timeout config to a function call, or call directly if no config."""
    if config is None or not config.enabled:
        return fn(*args, **kwargs)
    return config.run(fn, *args, **kwargs)
