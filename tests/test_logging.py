import logging
import pytest
from batchflow.logging import PipelineLogger, get_logger


class TestPipelineLogger:
    def test_defaults(self):
        logger = PipelineLogger()
        assert logger.name == "batchflow"
        assert logger.level == logging.INFO

    def test_custom_name(self):
        logger = PipelineLogger(name="myapp")
        assert logger.name == "myapp"

    def test_custom_level(self):
        logger = PipelineLogger(name="test_level", level=logging.DEBUG)
        assert logger.level == logging.DEBUG

    def test_info_logs(self, capfd):
        logger = PipelineLogger(name="test_info")
        logger.info("hello world")
        out, _ = capfd.readouterr()
        assert "hello world" in out

    def test_warning_logs(self, capfd):
        logger = PipelineLogger(name="test_warn")
        logger.warning("watch out")
        out, _ = capfd.readouterr()
        assert "watch out" in out

    def test_error_logs(self, capfd):
        logger = PipelineLogger(name="test_err")
        logger.error("something failed")
        out, _ = capfd.readouterr()
        assert "something failed" in out

    def test_log_item(self, capfd):
        logger = PipelineLogger(name="test_item")
        logger.log_item("abc", "processed", "ok")
        out, _ = capfd.readouterr()
        assert "PROCESSED" in out
        assert "abc" in out
        assert "ok" in out

    def test_log_item_no_detail(self, capfd):
        logger = PipelineLogger(name="test_item2")
        logger.log_item(42, "skipped")
        out, _ = capfd.readouterr()
        assert "SKIPPED" in out
        assert "42" in out

    def test_log_batch(self, capfd):
        logger = PipelineLogger(name="test_batch")
        logger.log_batch(3, 10)
        out, _ = capfd.readouterr()
        assert "batch 3" in out
        assert "10" in out

    def test_get_logger(self):
        logger = get_logger("helper_test", logging.WARNING)
        assert isinstance(logger, PipelineLogger)
        assert logger.name == "helper_test"
        assert logger.level == logging.WARNING
