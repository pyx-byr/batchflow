"""Tests for CheckpointPolicy."""
import time
import pytest
from batchflow.checkpoint_policy import CheckpointPolicy, apply_checkpoint_policy


class TestCheckpointPolicy:
    def test_defaults(self):
        policy = CheckpointPolicy()
        assert policy.name == "checkpoint_policy"
        assert policy.every_n_items is None
        assert policy.every_n_seconds is None
        assert policy.on_error is True
        assert policy.on_complete is True
        assert policy.condition is None

    def test_custom_name(self):
        policy = CheckpointPolicy(name="my_policy")
        assert policy.name == "my_policy"

    def test_invalid_every_n_items_zero(self):
        with pytest.raises(ValueError, match="every_n_items"):
            CheckpointPolicy(every_n_items=0)

    def test_invalid_every_n_items_negative(self):
        with pytest.raises(ValueError, match="every_n_items"):
            CheckpointPolicy(every_n_items=-5)

    def test_invalid_every_n_seconds_zero(self):
        with pytest.raises(ValueError, match="every_n_seconds"):
            CheckpointPolicy(every_n_seconds=0)

    def test_invalid_every_n_seconds_negative(self):
        with pytest.raises(ValueError, match="every_n_seconds"):
            CheckpointPolicy(every_n_seconds=-1.0)

    def test_every_n_items_triggers(self):
        policy = CheckpointPolicy(every_n_items=3)
        assert policy.should_checkpoint() is False  # 1
        assert policy.should_checkpoint() is False  # 2
        assert policy.should_checkpoint() is True   # 3
        assert policy.should_checkpoint() is False  # 4
        assert policy.should_checkpoint() is False  # 5
        assert policy.should_checkpoint() is True   # 6

    def test_every_n_seconds_triggers(self):
        policy = CheckpointPolicy(every_n_seconds=0.05)
        assert policy.should_checkpoint() is True   # first call, elapsed >= 0.05 from epoch
        time.sleep(0.06)
        assert policy.should_checkpoint() is True

    def test_condition_fn_triggers(self):
        policy = CheckpointPolicy(condition=lambda count, item: item == "save")
        assert policy.should_checkpoint("skip") is False
        assert policy.should_checkpoint("save") is True

    def test_reset_clears_count(self):
        policy = CheckpointPolicy(every_n_items=2)
        policy.should_checkpoint()  # 1
        policy.reset()
        assert policy._item_count == 0
        assert policy.should_checkpoint() is False  # 1 again after reset
        assert policy.should_checkpoint() is True   # 2

    def test_mark_checkpoint_chaining(self):
        policy = CheckpointPolicy()
        result = policy.mark_checkpoint()
        assert result is policy

    def test_apply_checkpoint_policy_none(self):
        assert apply_checkpoint_policy(None) is False

    def test_apply_checkpoint_policy_with_policy(self):
        policy = CheckpointPolicy(every_n_items=1)
        assert apply_checkpoint_policy(policy, "item") is True
