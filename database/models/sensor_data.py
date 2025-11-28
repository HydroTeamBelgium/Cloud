from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

@dataclass
class SensorData():
    id: int
    value : float
    timestamp : datetime =  field(default_factory=datetime.now)
    event : Optional[int] = None
    sensor_entity: Optional[int] = None
    measurement_type: Optional[int] = None

    def __post_init__(self):
        if not isinstance(self.timestamp, datetime):
            try:
                self.timestamp = datetime.strptime(self.timestamp.strip(), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise ValueError(f"Invalid date format for timestamp: {self.timestamp}. Expected 'YYYY-MM-DD HH:MM:SS'")
