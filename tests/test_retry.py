"""Tests for retry logic."""

import pytest
from unittest.mock import MagicMock

from batchflow.retry import RetryConfig, with_retry, retry_call


class TestRetryConfig:
    def test_defaults(self):
        cfg = RetryConfig()
        assert cfg.max_attempts == 3
        assert cfg.delay == 1.0
        assert cfg.backoff == 2.0
        assert cfg.exceptions == (Exception,)

    def test_custom(self):
        cfg = RetryConfig(max_attempts=5, delay=0.5, backoff=1.5)
        assert cfg.max_attempts == 5


class TestWithRetry:
    def test_success_on_first_attempt(self):
        mock_fn = MagicMock(return_value=42)
        cfg = RetryConfig(max_attempts=3, delay=0)
        result = with_retry(cfg)(mock_fn)()
        assert result == 42
        assert mock_fn.call_count == 1

    def test_retries_on_failure_then_succeeds(self):
        mock_fn = MagicMock(side_effect=[ValueError("fail"), ValueError("fail"), 99])
        cfg = RetryConfig(max_attempts=3, delay=0)
        result = with_retry(cfg)(mock_fn)()
        assert result == 99
        assert mock_fn.call_count == 3

    def test_raises_after_max_attempts(self):
        mock_fn = MagicMock(side_effect=RuntimeError("always fails"))
        cfg = RetryConfig(max_attempts=3, delay=0)
        with pytest.raises(RuntimeError):
            with_retry(cfg)(mock_fn)()
        assert mock_fn.call_count == 3

    def test_only_catches_specified_exceptions(self):
        mock_fn = MagicMock(side_effect=TypeError("wrong type"))
        cfg = RetryConfig(max_attempts=3, delay=0, exceptions=(ValueError,))
        with pytest.raises(TypeError):
            with_retry(cfg)(mock_fn)()
        assert mock_fn.call_count == 1


class TestRetryCall:
    def test_basic_call(self):
        fn = lambda x: x * 2
        result = retry_call(fn, args=(5,), config=RetryConfig(max_attempts=1, delay=0))
        assert result == 10

    def test_with_kwargs(self):
        fn = lambda x, y=1: x + y
        result = retry_call(fn, args=(3,), kwargs={"y": 4}, config=RetryConfig(max_attempts=1, delay=0))
        assert result == 7
