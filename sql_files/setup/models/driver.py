from dataclasses import dataclass
from datetime import datetime

@dataclass
class Driver:
    code: str
    given_name: str
    date_of_birth: datetime
