from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum
from database.models import Model

class precipitationType(Enum):
    fog = 'fog'
    rain = 'rain'
    hail = 'hail'
    snow = 'snow'

class roadCondition(Enum):
    water0 = 'water0'
    water1 = 'water1'
    water2 = 'water2'
    snow = 'snow'
    ice = 'ice'

@dataclass
class WeatherSensorData():
    id: int
    precipation_mm: Optional[float] = None
    precipitation_type: Optional[precipitationType] = None
    road_condition: Optional[roadCondition] = None
    wind_direction_degrees: Optional[float] = None
    wind_strength_mps: Optional[float] = None
    uv_index: Optional[float] = None
    temperature: Optional[float] = None
    timestamp : datetime =  field(default_factory=datetime.now)
    event : Optional[int] = None
    sensor_entity: Optional[int] = None

    def __post_init__(self):
        if not isinstance(self.timestamp, datetime):
            try:
                self.timestamp = datetime.strptime(self.timestamp.strip(), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise ValueError(f"Invalid date format for timestamp: {self.timestamp}. Expected 'YYYY-MM-DD HH:MM:SS'")

        # Validate precipation_type
        if isinstance(self.precipitation_type, str):
            try:
                self.precipitation_type = precipitationType(self.precipitation_type)
            except ValueError:
                raise ValueError(f"Invalid precipitation_type: {self.precipitation_type}, must be 'fog', 'rain', 'hail' or 'snow'")
            
        # Validate road_condition
        if isinstance(self.road_condition, str):
            try:
                self.road_condition = roadCondition(self.road_condition)
            except ValueError:
                raise ValueError(f"Invalid road_condition: {self.road_condition}, must be 'water0', 'water1', 'water2', 'snow' or 'ice'")