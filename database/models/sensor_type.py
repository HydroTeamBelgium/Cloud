
from dataclasses import dataclass

from database.models import Model


@dataclass
class SensorType():
  
    id : int
    manufacturer : int
    model : str
    sample_freq: float
