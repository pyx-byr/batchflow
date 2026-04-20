import threading
import time
import pytest
from batchflow.pause import PauseControl, apply_pause


class TestPauseControl:
    def test_defaults(self):
        pc = PauseControl()
        assert pc.name == "pause_control"
        assert pc.is_paused is False

    def test_custom_name(self):
        pc = PauseControl(name="my_pause")
        assert pc.name == "my_pause"

    def test_pause_sets_paused(self):
        pc = PauseControl()
        pc.pause()
        assert pc.is_paused is True

    def test_resume_clears_paused(self):
        pc = PauseControl()
        pc.pause()
        pc.resume()
        assert pc.is_paused is False

    def test_pause_chaining(self):
        pc = PauseControl()
        result = pc.pause()
        assert result is pc

    def test_resume_chaining(self):
        pc = PauseControl()
        result = pc.pause().resume()
        assert result is pc

    def test_wait_if_paused_not_paused(self):
        pc = PauseControl()
        result = pc.wait_if_paused(timeout=1.0)
        assert result is True

    def test_wait_if_paused_times_out(self):
        pc = PauseControl()
        pc.pause()
        result = pc.wait_if_paused(timeout=0.05)
        assert result is False

    def test_resume_unblocks_wait(self):
        pc = PauseControl()
        pc.pause()
        results = []

        def waiter():
            results.append(pc.wait_if_paused(timeout=2.0))

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)
        pc.resume()
        t.join(timeout=1.0)
        assert results == [True]

    def test_repr(self):
        pc = PauseControl(name="test")
        assert "test" in repr(pc)
        assert "paused" in repr(pc)


class TestApplyPause:
    def test_none_control_returns_true(self):
        assert apply_pause(None) is True

    def test_not_paused_returns_true(self):
        pc = PauseControl()
        assert apply_pause(pc) is True

    def test_paused_times_out(self):
        pc = PauseControl()
        pc.pause()
        result = apply_pause(pc, timeout=0.05)
        assert result is False
