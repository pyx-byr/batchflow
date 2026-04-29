import time
import pytest
from batchflow.profiler import ProfilerConfig, apply_profiler


class TestProfilerConfig:
    def test_defaults(self):
        p = ProfilerConfig()
        assert p.name == "profiler"
        assert p.stages() == []

    def test_custom_name(self):
        p = ProfilerConfig(name="my_profiler")
        assert p.name == "my_profiler"

    def test_start_stop_records_elapsed(self):
        p = ProfilerConfig()
        p.start("step")
        time.sleep(0.01)
        p.stop("step")
        assert p.elapsed("step") >= 0.01

    def test_count_increments(self):
        p = ProfilerConfig()
        for _ in range(3):
            p.start("step")
            p.stop("step")
        assert p.count("step") == 3

    def test_elapsed_unknown_stage_returns_zero(self):
        p = ProfilerConfig()
        assert p.elapsed("nonexistent") == 0.0

    def test_count_unknown_stage_returns_zero(self):
        p = ProfilerConfig()
        assert p.count("nonexistent") == 0

    def test_average_returns_none_when_no_calls(self):
        p = ProfilerConfig()
        assert p.average("step") is None

    def test_average_is_mean_elapsed(self):
        p = ProfilerConfig()
        p.start("s")
        p.stop("s")
        p.start("s")
        p.stop("s")
        avg = p.average("s")
        total = p.elapsed("s")
        assert avg == pytest.approx(total / 2, rel=1e-6)

    def test_stages_lists_recorded_stages(self):
        p = ProfilerConfig()
        p.start("a"); p.stop("a")
        p.start("b"); p.stop("b")
        assert set(p.stages()) == {"a", "b"}

    def test_reset_clears_all(self):
        p = ProfilerConfig()
        p.start("x"); p.stop("x")
        p.reset()
        assert p.stages() == []
        assert p.elapsed("x") == 0.0

    def test_reset_returns_self(self):
        p = ProfilerConfig()
        assert p.reset() is p

    def test_stop_without_start_raises(self):
        p = ProfilerConfig()
        with pytest.raises(KeyError, match="step"):
            p.stop("step")

    def test_summary_structure(self):
        p = ProfilerConfig()
        p.start("load"); p.stop("load")
        s = p.summary()
        assert "load" in s
        assert "elapsed" in s["load"]
        assert "count" in s["load"]
        assert "average" in s["load"]
        assert s["load"]["count"] == 1


class TestApplyProfiler:
    def test_applies_fn_and_records(self):
        p = ProfilerConfig()
        result = apply_profiler(p, "transform", lambda x: x * 2, 5)
        assert result == 10
        assert p.count("transform") == 1
        assert p.elapsed("transform") >= 0.0

    def test_none_profiler_skips_timing(self):
        result = apply_profiler(None, "transform", lambda x: x + 1, 9)
        assert result == 10
