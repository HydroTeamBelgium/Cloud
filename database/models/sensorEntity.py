
from dataclasses import dataclass
from datetime import datetime

from database.models import Model


@dataclass
class SensorEntity():
  
    id : int
    serial_number : int
    purchase_date : datetime
    sensor_type : str
    reading_end_point : int
    sensor_table : str

