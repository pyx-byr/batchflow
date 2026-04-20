from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional
import json
import pickle


@dataclass
class SerializerConfig:
    name: str = "serializer"
    format: str = "json"  # "json" or "pickle"
    _encoders: Dict[str, Callable] = field(default_factory=dict, repr=False)
    _decoders: Dict[str, Callable] = field(default_factory=dict, repr=False)

    def add_encoder(self, type_name: str, fn: Callable) -> "SerializerConfig":
        self._encoders[type_name] = fn
        return self

    def add_decoder(self, type_name: str, fn: Callable) -> "SerializerConfig":
        self._decoders[type_name] = fn
        return self

    def serialize(self, data: Any) -> bytes:
        if self.format == "pickle":
            return pickle.dumps(data)
        elif self.format == "json":
            return json.dumps(data, default=self._default_encoder).encode("utf-8")
        else:
            raise ValueError(f"Unsupported format: {self.format}")

    def deserialize(self, raw: bytes) -> Any:
        if self.format == "pickle":
            return pickle.loads(raw)
        elif self.format == "json":
            return json.loads(raw.decode("utf-8"))
        else:
            raise ValueError(f"Unsupported format: {self.format}")

    def _default_encoder(self, obj: Any) -> Any:
        type_name = type(obj).__name__
        if type_name in self._encoders:
            return self._encoders[type_name](obj)
        raise TypeError(f"Object of type {type_name} is not JSON serializable")


def apply_serializer(serializer: Optional[SerializerConfig], data: Any) -> bytes:
    if serializer is None:
        return json.dumps(data).encode("utf-8")
    return serializer.serialize(data)
