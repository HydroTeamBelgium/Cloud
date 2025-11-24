from dataclasses import dataclass
from typing import Optional
from database.models import Model

@dataclass
class CarComponent():
    id: int
    semantic_type: int
    manufacturer: Optional[int] = None
    serial_number: Optional[str] = None
    parent_component: Optional[int] = None
    car_version: Optional[int] = None


