from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time


@dataclass
class MetricsCollector:
    """Collects and reports pipeline execution metrics."""
    name: str = "default"
    _timings: List[float] = field(default_factory=list, init=False)
    _counts: Dict[str, int] = field(default_factory=dict, init=False)
    _start_time: Optional[float] = field(default=None, init=False)

    def start_timer(self) -> None:
        self._start_time = time.monotonic()

    def stop_timer(self) -> float:
        if self._start_time is None:
            return 0.0
        elapsed = time.monotonic() - self._start_time
        self._timings.append(elapsed)
        self._start_time = None
        return elapsed

    def increment(self, key: str, amount: int = 1) -> None:
        self._counts[key] = self._counts.get(key, 0) + amount

    def get(self, key: str, default: int = 0) -> int:
        return self._counts.get(key, default)

    @property
    def total_time(self) -> float:
        return sum(self._timings)

    @property
    def avg_time(self) -> float:
        if not self._timings:
            return 0.0
        return self.total_time / len(self._timings)

    @property
    def sample_count(self) -> int:
        return len(self._timings)

    def summary(self) -> Dict:
        return {
            "name": self.name,
            "counts": dict(self._counts),
            "total_time": round(self.total_time, 4),
            "avg_time": round(self.avg_time, 4),
            "samples": self.sample_count,
        }

    def reset(self) -> None:
        self._timings.clear()
        self._counts.clear()
        self._start_time = None
