from typing import Any, Dict, Type


class SingletonMeta(type):
    """
    A metaclass that implements the Singleton pattern to ensure only one instance of a class exists.
    
    This is used to guarantee that only one instance of a class exists across the application,
    preventing duplicate instances or conflicting setups.
    """
    _instances: Dict[Type, Any] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """
        Override the class instantiation to return an existing instance if one exists,
        otherwise create and store a new instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls] 