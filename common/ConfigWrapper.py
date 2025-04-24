
import yaml
from yaml.loader import SafeLoader
from typing import Dict, Any, Optional

import logging

from common.LoggerSingleton import SingletonMeta
from database.custom_exceptions import ConfigReadError

logger = logging.getLogger(__name__)

class ConfigWrapper(metaclass=SingletonMeta):
    """
    The ConfigWrapper class provides a wrapper for a YAML configuration file.
    """
    
    def __init__(self, config_dict: Optional[dict] = None, file_name: Optional[str] = None):
        if config_dict:
            self._loaded_config_dictionary = config_dict
        elif file_name:
            logger.warning("configuration should be instantiated from ConfigFactory, not loaded directly into ConfigWrapper")
            with open(f"config/{file_name}.yaml", "r") as config:
                try:
                    self._loaded_config_dictionary = yaml.load(config, Loader=SafeLoader)
                except yaml.YAMLError as e:
                    raise ConfigReadError(f"Unable to load config file, error:\n{e}")
        else:
            raise ValueError("Either config_dict or file_name must be provided")

    def get_project_id(self) -> str:
        return self._loaded_config_dictionary.get('project_id')

    def get_SQL_instance_region(self) ->str:
        return self._loaded_config_dictionary.get("database_region")

    def get_SQL_instance_name(self) -> str:
        return self._loaded_config_dictionary.get("database_instance_name")

    def get_SQL_instance_connection_name(self) -> str:
        return f"{self.get_project_id()}:{self.get_SQL_instance_region()}:{self.get_SQL_instance_name()}"

    def get_database_role_user_name(self) -> str:
        return self._loaded_config_dictionary.get("database_user").get("user_name")

    def get_database_role_password(self) -> str:
        return self._loaded_config_dictionary.get("database_user").get("password")

    def get_database_name(self) -> str:
        return self._loaded_config_dictionary.get("database_user").get("database_name")
    
    def get_config_param(self, param_name: str):
        """
        Retrieves a parameter from the loaded config dictionary.
        
        Args:
            param_name (str): The key for the desired config value. 
                              Nested keys can be accessed using dot notation (e.g., 'database_user.user_name').
        
        Returns:
            The value or section corresponding to the provided parameter name.
        
        Raises:
            KeyError: If the parameter is not found in the config.
        """
        try:
            keys = param_name.split(".")
            value = self._loaded_config_dictionary
            for key in keys:
                value = value[key]
            return value
        except (KeyError) as e:
            raise KeyError(f"Parameter '{param_name}' not found in the configuration file") from e