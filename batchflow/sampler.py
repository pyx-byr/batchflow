from dataclasses import dataclass, field
from typing import Callable, Optional
import random


@dataclass
class SamplerConfig:
    """Configuration for probabilistic or nth-item sampling of pipeline items."""

    name: str = "sampler"
    _rate: float = field(default=1.0, init=False, repr=False)
    _every_n: Optional[int] = field(default=None, init=False, repr=False)
    _seed: Optional[int] = field(default=None, init=False, repr=False)
    _rng: random.Random = field(default=None, init=False, repr=False)
    _counter: int = field(default=0, init=False, repr=False)

    def __post_init__(self):
        self._rng = random.Random(self._seed)

    def set_rate(self, rate: float) -> "SamplerConfig":
        """Set a probability rate (0.0–1.0) for sampling items."""
        if not (0.0 <= rate <= 1.0):
            raise ValueError(f"rate must be between 0.0 and 1.0, got {rate}")
        self._rate = rate
        return self

    def set_every_n(self, n: int) -> "SamplerConfig":
        """Sample every nth item (1-based). Overrides rate-based sampling."""
        if n < 1:
            raise ValueError(f"every_n must be >= 1, got {n}")
        self._every_n = n
        return self

    def set_seed(self, seed: int) -> "SamplerConfig":
        """Set a random seed for reproducible sampling."""
        self._seed = seed
        self._rng = random.Random(seed)
        return self

    def should_sample(self, item=None) -> bool:
        """Return True if the current item should be included in the sample."""
        self._counter += 1
        if self._every_n is not None:
            return self._counter % self._every_n == 0
        return self._rng.random() < self._rate

    def reset(self) -> "SamplerConfig":
        """Reset internal counter and re-seed the RNG."""
        self._counter = 0
        self._rng = random.Random(self._seed)
        return self

    @property
    def rate(self) -> float:
        return self._rate

    @property
    def every_n(self) -> Optional[int]:
        return self._every_n


def apply_sampler(sampler: Optional[SamplerConfig], item):
    """Return item if it passes the sampler, else None."""
    if sampler is None:
        return item
    return item if sampler.should_sample(item) else None
