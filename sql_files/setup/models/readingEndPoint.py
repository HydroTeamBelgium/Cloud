from dataclasses import dataclass
from typing import Optional

@dataclass
class ReadingEndPoint:
    """
    Represents a reading endpoint with its attributes.
    Attributes:
        id (int): Unique identifier for the reading endpoint.
        name (str): Name of the reading endpoint.
        functional_group (str): Functional group of the reading endpoint.
        car_component (Optional[int]): ID of the associated car component, if any.
    """
    
    id: int
    name: str
    functional_group: str
    car_component: Optional[int]
