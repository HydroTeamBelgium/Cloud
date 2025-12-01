from dataclasses import dataclass
from typing import Optional

@dataclass
class CarComponent():
    id: int
    semantic_type: int
    manufacturer: Optional[int] = None
    serial_number: Optional[str] = None
    parent_component: Optional[int] = None
    car_version: Optional[int] = None
