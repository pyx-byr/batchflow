from typing import Any, Callable, List, Optional
from dataclasses import dataclass, field


@dataclass
class TransformConfig:
    name: str = "transform"
    transforms: List[Callable[[Any], Any]] = field(default_factory=list)
    skip_on_error: bool = False

    def add(self, fn: Callable[[Any], Any]) -> "TransformConfig":
        self.transforms.append(fn)
        return self

    def apply(self, item: Any) -> Optional[Any]:
        result = item
        for fn in self.transforms:
            try:
                result = fn(result)
            except Exception as e:
                if self.skip_on_error:
                    return None
                raise RuntimeError(
                    f"Transform '{fn.__name__}' failed on item '{item}': {e}"
                ) from e
        return result


def apply_transforms(item: Any, config: TransformConfig) -> Optional[Any]:
    return config.apply(item)
