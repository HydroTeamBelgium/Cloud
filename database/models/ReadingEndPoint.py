from dataclasses import dataclass
from typing import Optional

@dataclass
class ReadingEndPoint:
    id: int
    name: str
    functional_group: str
    car_component: Optional[int]
