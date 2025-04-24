
from dataclasses import dataclass

from database.models import Model


@dataclass
class User(Model):
    
        id: int
        username : str
        email :str
        admin :str
        password :str
        active_session : bool
