
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SensorEntity(Model):
  
    id : int
    serial_number : int
    purchase_date : datetime
    sensor_type : str
    reading_end_point : int
    sensor_table : str

