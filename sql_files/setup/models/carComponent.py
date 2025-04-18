from dataclasses import dataclass
from typing import Optional

@dataclass
class CarComponent:
    """
    Represents a car component with its attributes.
    Attributes:
        id (int): Unique identifier for the car component.
        semantic_type (str): Semantic type of the car component.
        manufacturer (Optional[str]): Manufacturer of the car component.
        serial_number (Optional[str]): Serial number of the car component.
        parent_component (Optional[int]): ID of the parent component, if any.
    """
    id: int
    semantic_type: str
    manufacturer: Optional[str]
    serial_number: Optional[str]
    parent_component: Optional[int]
