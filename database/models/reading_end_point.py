from dataclasses import dataclass
from typing import Optional

@dataclass
class ReadingEndPoint():
    id: int
    name: int
    car_component: Optional[int] = None
    description: Optional[str] = None