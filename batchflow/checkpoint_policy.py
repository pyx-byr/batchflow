"""Checkpoint policy configuration for controlling when checkpoints are saved."""
from dataclasses import dataclass, field
from typing import Optional, Callable


@dataclass
class CheckpointPolicy:
    """Controls when and how often checkpoints are saved during pipeline execution."""

    name: str = "checkpoint_policy"
    every_n_items: Optional[int] = None
    every_n_seconds: Optional[float] = None
    on_error: bool = True
    on_complete: bool = True
    condition: Optional[Callable[[int, object], bool]] = None

    _item_count: int = field(default=0, init=False, repr=False)
    _last_checkpoint_time: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self):
        if self.every_n_items is not None and self.every_n_items <= 0:
            raise ValueError("every_n_items must be a positive integer")
        if self.every_n_seconds is not None and self.every_n_seconds <= 0:
            raise ValueError("every_n_seconds must be a positive number")

    def reset(self) -> "CheckpointPolicy":
        """Reset internal counters."""
        self._item_count = 0
        self._last_checkpoint_time = 0.0
        return self

    def should_checkpoint(self, item: object = None) -> bool:
        """Determine whether a checkpoint should be saved now."""
        import time

        self._item_count += 1

        if self.condition is not None and self.condition(self._item_count, item):
            return True

        if self.every_n_items is not None and self._item_count % self.every_n_items == 0:
            return True

        if self.every_n_seconds is not None:
            now = time.monotonic()
            if now - self._last_checkpoint_time >= self.every_n_seconds:
                self._last_checkpoint_time = now
                return True

        return False

    def mark_checkpoint(self) -> "CheckpointPolicy":
        """Manually record that a checkpoint was taken."""
        import time
        self._last_checkpoint_time = time.monotonic()
        return self


def apply_checkpoint_policy(policy: Optional[CheckpointPolicy], item: object = None) -> bool:
    """Return True if a checkpoint should be saved, False otherwise."""
    if policy is None:
        return False
    return policy.should_checkpoint(item)
