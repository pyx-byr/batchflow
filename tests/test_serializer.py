import pytest
from batchflow.serializer import SerializerConfig, apply_serializer
import json


class TestSerializerConfig:
    def test_defaults(self):
        s = SerializerConfig()
        assert s.name == "serializer"
        assert s.format == "json"

    def test_custom_name(self):
        s = SerializerConfig(name="my_serializer")
        assert s.name == "my_serializer"

    def test_json_serialize_deserialize(self):
        s = SerializerConfig(format="json")
        data = {"key": "value", "num": 42}
        raw = s.serialize(data)
        assert isinstance(raw, bytes)
        result = s.deserialize(raw)
        assert result == data

    def test_pickle_serialize_deserialize(self):
        s = SerializerConfig(format="pickle")
        data = {"key": "value", "num": 42}
        raw = s.serialize(data)
        assert isinstance(raw, bytes)
        result = s.deserialize(raw)
        assert result == data

    def test_invalid_format_serialize(self):
        s = SerializerConfig(format="xml")
        with pytest.raises(ValueError, match="Unsupported format"):
            s.serialize({"a": 1})

    def test_invalid_format_deserialize(self):
        s = SerializerConfig(format="xml")
        with pytest.raises(ValueError, match="Unsupported format"):
            s.deserialize(b"<a>1</a>")

    def test_custom_encoder(self):
        s = SerializerConfig(format="json")
        s.add_encoder("set", lambda v: list(v))
        raw = s.serialize({"items": {1, 2, 3}})
        result = s.deserialize(raw)
        assert sorted(result["items"]) == [1, 2, 3]

    def test_add_encoder_chaining(self):
        s = SerializerConfig()
        result = s.add_encoder("bytes", lambda v: v.decode())
        assert result is s

    def test_add_decoder_chaining(self):
        s = SerializerConfig()
        result = s.add_decoder("str", lambda v: v)
        assert result is s

    def test_unserializable_raises(self):
        s = SerializerConfig(format="json")
        with pytest.raises(TypeError):
            s.serialize({"fn": lambda x: x})


class TestApplySerializer:
    def test_apply_with_none(self):
        raw = apply_serializer(None, {"a": 1})
        assert json.loads(raw.decode()) == {"a": 1}

    def test_apply_with_config(self):
        s = SerializerConfig(format="pickle")
        raw = apply_serializer(s, [1, 2, 3])
        import pickle
        assert pickle.loads(raw) == [1, 2, 3]
