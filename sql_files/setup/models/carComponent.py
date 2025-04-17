from dataclasses import dataclass
from typing import Optional

@dataclass
class CarComponent:
    id: int
    semantic_type: str
    manufacturer: Optional[str]
    serial_number: Optional[str]
    parent_component: Optional[int]
