from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from database.models import Model

@dataclass
class Event():
    id: int
    name: str
    start_date: datetime
    end_date: datetime
    location: int
    description: Optional[str] = None
    track: Optional[str] = None
    static: Optional[bool] = False
    driver: Optional[int] = None
    event_type: Optional[int] = None



    def __post_init__(self):
        if not isinstance(self.start_date, datetime):
            try:
                self.start_date = datetime.strptime(self.start_date.strip(), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise ValueError(f"Invalid date format for start_date: {self.start_date}. Expected 'YYYY-MM-DD HH:MM:SS'")

        if not isinstance(self.end_date, datetime):
            try:
                self.end_date = datetime.strptime(self.end_date.strip(), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise ValueError(f"Invalid date format for end_date: {self.end_date}. Expected 'YYYY-MM-DD HH:MM:SS'")
