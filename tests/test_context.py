import time
import pytest
from batchflow.context import PipelineContext


class TestPipelineContext:
    def test_defaults(self):
        ctx = PipelineContext()
        assert ctx.run_id == ""
        assert ctx.metadata == {}
        assert ctx.tags == {}
        assert ctx.elapsed is None

    def test_custom_run_id(self):
        ctx = PipelineContext(run_id="abc-123")
        assert ctx.run_id == "abc-123"

    def test_set_and_get(self):
        ctx = PipelineContext()
        result = ctx.set("key", 42)
        assert result is ctx
        assert ctx.get("key") == 42

    def test_get_default(self):
        ctx = PipelineContext()
        assert ctx.get("missing", "default") == "default"

    def test_tag(self):
        ctx = PipelineContext()
        result = ctx.tag("env", "prod")
        assert result is ctx
        assert ctx.tags["env"] == "prod"

    def test_start_returns_self(self):
        ctx = PipelineContext()
        result = ctx.start()
        assert result is ctx

    def test_stop_returns_self(self):
        ctx = PipelineContext()
        ctx.start()
        result = ctx.stop()
        assert result is ctx

    def test_elapsed_none_before_start(self):
        ctx = PipelineContext()
        assert ctx.elapsed is None

    def test_elapsed_after_start(self):
        ctx = PipelineContext()
        ctx.start()
        time.sleep(0.05)
        assert ctx.elapsed is not None
        assert ctx.elapsed >= 0.04

    def test_elapsed_after_stop(self):
        ctx = PipelineContext()
        ctx.start()
        time.sleep(0.05)
        ctx.stop()
        elapsed = ctx.elapsed
        time.sleep(0.05)
        assert ctx.elapsed == elapsed  # frozen after stop

    def test_repr(self):
        ctx = PipelineContext(run_id="x1")
        ctx.set("foo", 1)
        ctx.tag("env", "test")
        r = repr(ctx)
        assert "x1" in r
        assert "env" in r
        assert "foo" in r
