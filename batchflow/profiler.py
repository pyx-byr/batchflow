from dataclasses import dataclass, field
from typing import Dict, Optional
import time


@dataclass
class ProfilerConfig:
    """Tracks per-stage timing and call counts for pipeline profiling."""

    name: str = "profiler"
    _timings: Dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _counts: Dict[str, int] = field(default_factory=dict, init=False, repr=False)
    _active: Dict[str, float] = field(default_factory=dict, init=False, repr=False)

    def start(self, stage: str) -> "ProfilerConfig":
        """Begin timing a named stage."""
        self._active[stage] = time.perf_counter()
        return self

    def stop(self, stage: str) -> "ProfilerConfig":
        """Stop timing a named stage and accumulate elapsed time."""
        if stage not in self._active:
            raise KeyError(f"Stage '{stage}' was not started.")
        elapsed = time.perf_counter() - self._active.pop(stage)
        self._timings[stage] = self._timings.get(stage, 0.0) + elapsed
        self._counts[stage] = self._counts.get(stage, 0) + 1
        return self

    def elapsed(self, stage: str) -> float:
        """Return total accumulated seconds for a stage."""
        return self._timings.get(stage, 0.0)

    def count(self, stage: str) -> int:
        """Return number of times a stage was recorded."""
        return self._counts.get(stage, 0)

    def average(self, stage: str) -> Optional[float]:
        """Return average time per call for a stage, or None if never called."""
        c = self._counts.get(stage, 0)
        if c == 0:
            return None
        return self._timings[stage] / c

    def stages(self) -> list:
        """Return list of all recorded stage names."""
        return list(self._timings.keys())

    def reset(self) -> "ProfilerConfig":
        """Clear all recorded timings and counts."""
        self._timings.clear()
        self._counts.clear()
        self._active.clear()
        return self

    def summary(self) -> Dict[str, dict]:
        """Return a summary dict of all stages with elapsed, count, and average."""
        return {
            stage: {
                "elapsed": self.elapsed(stage),
                "count": self.count(stage),
                "average": self.average(stage),
            }
            for stage in self.stages()
        }


def apply_profiler(profiler: Optional[ProfilerConfig], stage: str, fn, item):
    """Run fn(item) wrapped in profiler timing for the given stage."""
    if profiler is None:
        return fn(item)
    profiler.start(stage)
    try:
        result = fn(item)
    finally:
        profiler.stop(stage)
    return result
