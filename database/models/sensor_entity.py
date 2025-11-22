from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional

@dataclass
class SensorEntity():
    id : int
    serial_number : str
    purchase_date : date
    sensor_type : Optional[int]
    reading_end_point : Optional[int]

    def __post_init__(self):
        if not isinstance(self.purchase_date, date):
            try:
                self.purchase_date = datetime.strptime(self.purchase_date.strip(), "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Invalid date format for purchase_date: {self.purchase_date}. Expected 'YYYY-MM-DD'")
