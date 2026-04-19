"""Retry logic for batch processing steps."""

import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(self, max_attempts=3, delay=1.0, backoff=2.0, exceptions=(Exception,)):
        self.max_attempts = max_attempts
        self.delay = delay
        self.backoff = backoff
        self.exceptions = exceptions


def with_retry(config: RetryConfig = None):
    """Decorator factory that wraps a function with retry logic."""
    if config is None:
        config = RetryConfig()

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = config.delay
            while attempt < config.max_attempts:
                try:
                    return fn(*args, **kwargs)
                except config.exceptions as e:
                    attempt += 1
                    if attempt >= config.max_attempts:
                        logger.error(
                            "Function '%s' failed after %d attempts: %s",
                            fn.__name__, attempt, e
                        )
                        raise
                    logger.warning(
                        "Attempt %d/%d for '%s' failed: %s. Retrying in %.1fs...",
                        attempt, config.max_attempts, fn.__name__, e, current_delay
                    )
                    time.sleep(current_delay)
                    current_delay *= config.backoff
        return wrapper
    return decorator


def retry_call(fn, args=(), kwargs=None, config: RetryConfig = None):
    """Call a function with retry logic without using the decorator."""
    if kwargs is None:
        kwargs = {}
    if config is None:
        config = RetryConfig()
    wrapped = with_retry(config)(fn)
    return wrapped(*args, **kwargs)
