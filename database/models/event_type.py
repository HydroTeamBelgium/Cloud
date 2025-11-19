from dataclasses import dataclass
from database.models import Model

@dataclass
class EventType():
    id: int
    event_type: int