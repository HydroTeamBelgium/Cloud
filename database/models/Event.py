from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from database.models import Model

@dataclass
class Event(Model):
    id: int
    name: str
    start_date: datetime
    end_date: datetime
    location: Optional[str]


    def __post_init__(self):
        if not isinstance(self.start_date, datetime):
            self.start_date = datetime.strptime(self.start_date.strip(), "%Y-%m-%d %H:%M:%S")

        if not isinstance(self.end_date, datetime):
            self.end_date = datetime.strptime(self.end_date.strip(), "%Y-%m-%d %H:%M:%S")