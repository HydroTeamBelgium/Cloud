from dataclasses import dataclass

@dataclass
class SensorType():
    id : int
    manufacturer : int
    model : str
    sample_freq: float
