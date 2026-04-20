from dataclasses import dataclass, field
import threading


@dataclass
class PauseControl:
    """Allows a pipeline to be paused and resumed during processing."""
    name: str = "pause_control"
    _paused: bool = field(default=False, init=False, repr=False)
    _event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)

    def __post_init__(self):
        self._event.set()  # not paused initially

    def pause(self) -> "PauseControl":
        """Pause processing."""
        self._paused = True
        self._event.clear()
        return self

    def resume(self) -> "PauseControl":
        """Resume processing."""
        self._paused = False
        self._event.set()
        return self

    def wait_if_paused(self, timeout: float = None) -> bool:
        """Block if paused. Returns True when resumed, False on timeout."""
        return self._event.wait(timeout=timeout)

    @property
    def is_paused(self) -> bool:
        return self._paused

    def __repr__(self) -> str:
        return f"PauseControl(name={self.name!r}, paused={self._paused})"


def apply_pause(control: PauseControl, timeout: float = None) -> bool:
    """Wait if the control is paused. Returns False if timed out."""
    if control is None:
        return True
    return control.wait_if_paused(timeout=timeout)
