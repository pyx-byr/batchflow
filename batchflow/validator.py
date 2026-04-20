from dataclasses import dataclass, field
from typing import Callable, Any, List, Optional


@dataclass
class ValidatorConfig:
    name: str = "validator"
    raise_on_error: bool = False
    _validators: List[dict] = field(default_factory=list, init=False, repr=False)

    def add(self, fn: Callable[[Any], bool], message: str = "Validation failed") -> "ValidatorConfig":
        self._validators.append({"fn": fn, "message": message})
        return self

    def validate(self, item: Any) -> tuple[bool, Optional[str]]:
        for v in self._validators:
            try:
                if not v["fn"](item):
                    if self.raise_on_error:
                        raise ValueError(v["message"])
                    return False, v["message"]
            except ValueError:
                raise
            except Exception as e:
                msg = f"{v['message']}: {e}"
                if self.raise_on_error:
                    raise ValueError(msg)
                return False, msg
        return True, None

    def is_valid(self, item: Any) -> bool:
        ok, _ = self.validate(item)
        return ok


def apply_validator(item: Any, validator: Optional[ValidatorConfig]) -> tuple[bool, Optional[str]]:
    if validator is None:
        return True, None
    return validator.validate(item)
