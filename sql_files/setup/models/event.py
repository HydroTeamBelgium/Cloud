from dataclasses import dataclass
from datetime import datetime

@dataclass
class Event:
    round: int
    raceName: str
    date: datetime
    time: str
