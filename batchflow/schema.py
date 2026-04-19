from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class SchemaField:
    name: str
    type: type
    required: bool = True
    validator: Optional[Callable[[Any], bool]] = None


@dataclass
class SchemaConfig:
    fields: List[SchemaField] = field(default_factory=list)
    name: str = "schema"
    strict: bool = False

    def add(self, name: str, type: type, required: bool = True,
            validator: Optional[Callable[[Any], bool]] = None) -> "SchemaConfig":
        self.fields.append(SchemaField(name=name, type=type, required=required, validator=validator))
        return self

    def validate(self, item: Dict[str, Any]) -> List[str]:
        errors = []
        for f in self.fields:
            if f.name not in item:
                if f.required:
                    errors.append(f"Missing required field: '{f.name}'")
                continue
            value = item[f.name]
            if not isinstance(value, f.type):
                errors.append(f"Field '{f.name}' expected {f.type.__name__}, got {type(value).__name__}")
                continue
            if f.validator and not f.validator(value):
                errors.append(f"Field '{f.name}' failed custom validation")
        if self.strict:
            known = {f.name for f in self.fields}
            for key in item:
                if key not in known:
                    errors.append(f"Unexpected field: '{key}'")
        return errors


def apply_schema(item: Dict[str, Any], schema: Optional[SchemaConfig]) -> List[str]:
    if schema is None:
        return []
    return schema.validate(item)
