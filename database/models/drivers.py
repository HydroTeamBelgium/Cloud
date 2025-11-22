from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional

class Sex(Enum):
    M = 'M'
    F = 'F'
    
    def __str__(self):
        return self.value

@dataclass
class Driver():
    id: int
    name: str
    dob: date
    weight: int
    height: int
    sex: Sex
    role: Optional[int] = None

    def __post_init__(self):
        # Parse dob if it comes as a string
        if not isinstance(self.dob, date):
            try:
                self.dob = datetime.strptime(self.dob.strip(), "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Invalid date format for dob: {self.dob}. Expected 'YYYY-MM-DD'")

        # Validate sex
        if isinstance(self.sex, str):
            try:
                self.sex = Sex(self.sex)
            except ValueError:
                raise ValueError(f"Invalid sex: {self.sex}, must be 'M' or 'F'")