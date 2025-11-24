
from dataclasses import dataclass

from database.models import Model


@dataclass
class SensorTypeMeasurementType():
  
    sensor_type_id : int
    measurement_type_id : int

