from dataclasses import dataclass
from datetime import datetime

from database.models import Model

@dataclass
class Driver(Model):
    id: str
    permanent_number: int
    code: str
    url: str
    given_name: str
    family_name: str
    date_of_birth: datetime
    nationality: str


    def __post_init__(self):
        if not isinstance(self.date_of_birth, datetime):
            self.date_of_birth = datetime.strptime(self.date_of_birth.strip(), "%Y-%m-%d %H:%M:%S")
