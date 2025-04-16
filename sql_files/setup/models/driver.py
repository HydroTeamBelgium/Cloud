from dataclasses import dataclass
from datetime import datetime

@dataclass
class Driver:
    id: str
    permanent_number: int
    code: str
    url: str
    given_name: str
    family_name: str
    date_of_birth: datetime
    nationality: str
