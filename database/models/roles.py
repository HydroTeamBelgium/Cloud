from dataclasses import dataclass

from database.models import Model

@dataclass
class ReadingEndPoint():
    id: int
    role: int
