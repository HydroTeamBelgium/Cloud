import hashlib
import uuid

from common.LoggerSingleton import SingletonMeta

class CredentialsFactory(metaclass=SingletonMeta):
    """
    CredentialsFactory is designed to generate unique and consistent UUIDs (Universally Unique Identifiers) for different types of entities
    It provides a hashing utility as well.
    """
    
    #uuid4 generated IDs (online), hardcoded once
    _namespace_user = uuid.UUID('c7f697e8-d8a2-45a0-b681-190b890d1f1c')
    _namespace_car_component = uuid.UUID('e278c475-a1df-46ba-8084-ebf941bc22e4')

    def __init__(self) -> None:
        pass
    
    @staticmethod
    def hash_string(str_to_hash: str) -> str:
        return hashlib.sha3_512(str_to_hash.encode("utf-8")).hexdigest()
    
    def get_user_id(self, username:str) -> str:
        return str(uuid.uuid5(self._namespace_user, username))
    
    def get_car_component_id(self):
        pass
    
    