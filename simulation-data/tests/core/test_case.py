from dataclasses import dataclass, field
from typing import Callable, Any, List

@dataclass
class TestCase:
    id: str
    phase: int
    rpc: str
    description: str
    requests: Callable[[], List[Any]]
    expected: dict
    tolerance_type: str
    tags: List[str] = field(default_factory=list)
