

from dataclasses import dataclass
from datetime import datetime
from database.models import Model

@dataclass
class SensorData(Model):

    sensor_id: int
    value : float
    timestamp : datetime
    event_id :int 


    def __post_init__(self):
        if not isinstance(self.timestamp, datetime):
            self.timestamp = datetime.strptime(self.timestamp.strip(), "%Y-%m-%d %H:%M:%S")
