from dataclasses import dataclass
from datetime import datetime

@dataclass
class Driver:
    """
    Represents a driver with their attributes.
    Attributes:
        code (str): Unique identifier for the driver.
        given_name (str): Given name of the driver.
        date_of_birth (datetime): Date of birth of the driver.
    """
    
    code: str
    given_name: str
    date_of_birth: datetime
