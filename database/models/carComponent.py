from dataclasses import dataclass
from typing import Optional

from database.models import Model

@dataclass
class CarComponent(Model):
    id: int
    semantic_type: str
    manufacturer: Optional[str]
    serial_number: Optional[str]
    parent_component: Optional[int]



