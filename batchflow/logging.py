from dataclasses import dataclass, field
from typing import Optional, Callable, List
import logging
import sys


@dataclass
class PipelineLogger:
    name: str = "batchflow"
    level: int = logging.INFO
    format: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers: List[logging.Handler] = field(default_factory=list)
    _logger: Optional[logging.Logger] = field(default=None, init=False, repr=False)

    def __post_init__(self):
        self._logger = logging.getLogger(self.name)
        self._logger.setLevel(self.level)
        if not self.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter(self.format))
            self.handlers.append(handler)
        for h in self.handlers:
            self._logger.addHandler(h)

    def info(self, msg: str):
        self._logger.info(msg)

    def warning(self, msg: str):
        self._logger.warning(msg)

    def error(self, msg: str):
        self._logger.error(msg)

    def debug(self, msg: str):
        self._logger.debug(msg)

    def log_item(self, item, status: str, detail: str = ""):
        suffix = f" — {detail}" if detail else ""
        self._logger.info(f"[{status.upper()}] item={item!r}{suffix}")

    def log_batch(self, batch_index: int, size: int):
        self._logger.info(f"Processing batch {batch_index} with {size} item(s)")


def get_logger(name: str = "batchflow", level: int = logging.INFO) -> PipelineLogger:
    return PipelineLogger(name=name, level=level)
