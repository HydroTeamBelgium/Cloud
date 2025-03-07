"""Central configuration utility for the entire codebase."""

import os
import yaml
import inspect
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

CONFIG_LOCATIONS = {
    'dataflow': 'dataflow/config.yaml',
    'pubsub': 'pubsub/config.yaml',
    'database': 'database/config.yaml',
}

DEFAULT_CONFIG_NAME = 'config.yaml'

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from a YAML file.
    
    If config_path is not provided, it will attempt to determine the appropriate
    config file based on the calling module's location.
    
    Args:
        config_path: Optional explicit path to the config file
        
    Returns:
        Dictionary containing the configuration
        
    Raises:
        FileNotFoundError: If the configuration file cannot be found
        yaml.YAMLError: If the configuration file is not valid YAML
    """
    if not config_path:
        frame = inspect.currentframe().f_back
        if frame:
            module = inspect.getmodule(frame)
            if module:
                # Get the module's filename
                module_path = module.__file__
                config_path = _determine_config_path(module_path)
                logger.debug(f"Automatically determined config path: {config_path}")
    
    if not config_path:
        raise ValueError("Could not determine config path and none was provided")
        
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
    logger.info(f"Loading configuration from {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
    
def _determine_config_path(module_path: str) -> Optional[str]:
    """Determine the appropriate config path based on the calling module's path.
    
    Args:
        module_path: Path to the module requesting the config
        
    Returns:
        Path to the appropriate config file, or None if it can't be determined
    """
    if not module_path:
        return None
        
    # Convert to absolute path and normalize
    abs_path = os.path.abspath(module_path)
    
    for prefix, config_loc in CONFIG_LOCATIONS.items():
        if f"/{prefix}/" in abs_path:
            if os.path.exists(config_loc):
                return config_loc
    
    module_dir = os.path.dirname(abs_path)
    default_config = os.path.join(module_dir, DEFAULT_CONFIG_NAME)
    if os.path.exists(default_config):
        return default_config

    parts = abs_path.split('/common/')
    if len(parts) > 1:
        project_root = parts[0]
        root_config = os.path.join(project_root, DEFAULT_CONFIG_NAME)
        if os.path.exists(root_config):
            return root_config
    
    return None

def get_section(section_name: str, config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load a specific section from the configuration.
    
    Args:
        section_name: Name of the section to load
        config_path: Optional explicit path to the config file
        
    Returns:
        Dictionary containing the requested section
        
    Raises:
        KeyError: If the requested section doesn't exist
    """
    config = load_config(config_path)
    if section_name not in config:
        raise KeyError(f"Section '{section_name}' not found in configuration")
    return config[section_name]
