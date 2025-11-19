from dataclasses import dataclass
from database.models import Model

@dataclass
class CarVersion():
    id: int
    version: int