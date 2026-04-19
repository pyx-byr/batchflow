from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import time


@dataclass
class PipelineContext:
    """Carries shared metadata and state across a pipeline run."""

    run_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    _start_time: Optional[float] = field(default=None, init=False, repr=False)
    _end_time: Optional[float] = field(default=None, init=False, repr=False)

    def start(self) -> "PipelineContext":
        """Mark the pipeline run as started."""
        self._start_time = time.monotonic()
        return self

    def stop(self) -> "PipelineContext":
        """Mark the pipeline run as stopped."""
        self._end_time = time.monotonic()
        return self

    @property
    def elapsed(self) -> Optional[float]:
        """Return elapsed seconds if both start and stop have been called."""
        if self._start_time is None:
            return None
        end = self._end_time if self._end_time is not None else time.monotonic()
        return end - self._start_time

    def set(self, key: str, value: Any) -> "PipelineContext":
        """Store an arbitrary metadata value."""
        self.metadata[key] = value
        return self

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a metadata value."""
        return self.metadata.get(key, default)

    def tag(self, key: str, value: str) -> "PipelineContext":
        """Attach a string tag."""
        self.tags[key] = value
        return self

    def __repr__(self) -> str:
        return (
            f"PipelineContext(run_id={self.run_id!r}, "
            f"tags={self.tags}, metadata_keys={list(self.metadata.keys())})"
        )
