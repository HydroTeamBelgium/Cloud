from dataclasses import dataclass
from typing import Optional

@dataclass
class Users():
        id: int
        username : str
        email :str
        authorisation: int
        password :str
        active_session : Optional[bool] = False
