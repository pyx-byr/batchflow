"""Progress tracking for batch pipelines."""
from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class ProgressTracker:
    """Tracks progress of a batch processing pipeline."""
    total: int
    description: str = "Processing"
    _processed: int = field(default=0, init=False)
    _failed: int = field(default=0, init=False)
    _skipped: int = field(default=0, init=False)
    _start_time: float = field(default_factory=time.time, init=False)

    def increment(self) -> None:
        self._processed += 1

    def increment_failed(self) -> None:
        self._failed += 1

    def increment_skipped(self) -> None:
        self._skipped += 1

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def failed(self) -> int:
        return self._failed

    @property
    def skipped(self) -> int:
        return self._skipped

    @property
    def elapsed(self) -> float:
        return time.time() - self._start_time

    @property
    def rate(self) -> Optional[float]:
        if self.elapsed == 0:
            return None
        return self._processed / self.elapsed

    @property
    def eta(self) -> Optional[float]:
        """Estimated seconds remaining based on current processing rate."""
        if self.rate is None or self.rate == 0:
            return None
        remaining = self.total - (self._processed + self._skipped)
        return remaining / self.rate

    @property
    def percent(self) -> float:
        if self.total == 0:
            return 0.0
        return (self._processed + self._skipped) / self.total * 100

    def summary(self) -> dict:
        return {
            "total": self.total,
            "processed": self._processed,
            "failed": self._failed,
            "skipped": self._skipped,
            "elapsed": round(self.elapsed, 3),
            "rate": round(self.rate, 3) if self.rate is not None else None,
            "percent": round(self.percent, 2),
            "eta": round(self.eta, 3) if self.eta is not None else None,
        }

    def __str__(self) -> str:
        eta_str = f" eta={self.eta:.1f}s" if self.eta is not None else ""
        return (
            f"{self.description}: {self.percent:.1f}% "
            f"({self._processed}/{self.total}) "
            f"failed={self._failed} skipped={self._skipped} "
            f"elapsed={self.elapsed:.1f}s{eta_str}"
        )
