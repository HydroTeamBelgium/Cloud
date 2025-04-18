from dataclasses import dataclass
from datetime import datetime

@dataclass
class Event:
    """
    Represents an event with its attributes.
    
    Attributes:

        round (int): Round number of the event.
        raceName (str): Title
        date (datetime): Date of the event.
        time (str): Time of the event in string format.
    """
    round: int
    raceName: str
    date: datetime
    time: str
