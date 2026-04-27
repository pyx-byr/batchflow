import pytest
from batchflow.partitioner import PartitionerConfig, apply_partitioner


class TestPartitionerConfig:
    def test_defaults(self):
        p = PartitionerConfig()
        assert p.name == "partitioner"
        assert p.partition_count == 0
        assert p.total_items == 0

    def test_custom_name(self):
        p = PartitionerConfig(name="my_partitioner")
        assert p.name == "my_partitioner"

    def test_set_key_chaining(self):
        p = PartitionerConfig()
        result = p.set_key(lambda x: str(x % 2))
        assert result is p

    def test_set_key_non_callable_raises(self):
        p = PartitionerConfig()
        with pytest.raises(TypeError, match="callable"):
            p.set_key("not_a_function")

    def test_partition_without_key_raises(self):
        p = PartitionerConfig()
        with pytest.raises(RuntimeError, match="No key function"):
            p.partition(42)

    def test_partition_assigns_item(self):
        p = PartitionerConfig()
        p.set_key(lambda x: "even" if x % 2 == 0 else "odd")
        key = p.partition(4)
        assert key == "even"
        assert p.get("even") == [4]

    def test_partition_multiple_items(self):
        p = PartitionerConfig()
        p.set_key(lambda x: "even" if x % 2 == 0 else "odd")
        for i in range(6):
            p.partition(i)
        assert sorted(p.get("even")) == [0, 2, 4]
        assert sorted(p.get("odd")) == [1, 3, 5]

    def test_keys(self):
        p = PartitionerConfig()
        p.set_key(lambda x: x["type"])
        p.partition({"type": "a", "val": 1})
        p.partition({"type": "b", "val": 2})
        assert set(p.keys()) == {"a", "b"}

    def test_partition_count(self):
        p = PartitionerConfig()
        p.set_key(lambda x: str(x % 3))
        for i in range(9):
            p.partition(i)
        assert p.partition_count == 3

    def test_total_items(self):
        p = PartitionerConfig()
        p.set_key(lambda x: "all")
        for i in range(5):
            p.partition(i)
        assert p.total_items == 5

    def test_get_missing_key_returns_empty(self):
        p = PartitionerConfig()
        p.set_key(lambda x: "x")
        assert p.get("nonexistent") == []

    def test_reset_clears_partitions(self):
        p = PartitionerConfig()
        p.set_key(lambda x: "bucket")
        p.partition(1)
        p.partition(2)
        p.reset()
        assert p.partition_count == 0
        assert p.total_items == 0

    def test_apply_partitioner(self):
        p = PartitionerConfig()
        p.set_key(lambda x: "hi" if x > 5 else "lo")
        result = apply_partitioner(p, [1, 3, 7, 9, 2])
        assert sorted(result["lo"]) == [1, 2, 3]
        assert sorted(result["hi"]) == [7, 9]

    def test_apply_partitioner_resets_before_run(self):
        p = PartitionerConfig()
        p.set_key(lambda x: "k")
        apply_partitioner(p, [1, 2])
        result = apply_partitioner(p, [3, 4])
        # Should only contain items from second call
        assert result["k"] == [3, 4]
