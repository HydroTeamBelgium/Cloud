from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Event:
    id: int
    name: str
    start_date: datetime
    end_date: datetime
    location: Optional[str]