from dataclasses import dataclass
from typing import Optional

from database.models import Model

@dataclass
class ReadingEndPoint():
    id: int
    name: str
    functional_group: str
    car_component: Optional[int]
