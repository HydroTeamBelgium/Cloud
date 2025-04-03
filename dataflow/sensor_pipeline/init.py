import argparse
import os
import sys
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

from common.logger import LoggerFactory


class PipelineInitializer:
    """Class for initializing and configuring the sensor data pipeline."""
    
    def __init__(self):
        """Initialize the PipelineInitializer with a logger."""
        logger_factory = LoggerFactory()
        self._logger = logger_factory.get_logger(__name__, log_file="sensor_pipeline.log")
    
    def _get_default_config_path(self) -> str:
        """Get the default configuration file path.
        
        Returns:
            str: Path to the default config file
        """
        if config_path := os.getenv('DATAFLOW_CONFIG_PATH'):
            return config_path
            
        base_dir = Path(__file__).resolve().parents[1]
        return str(base_dir / 'config.yaml')

    def _load_yaml_config(self, config_path: str) -> Dict[str, Any]:
        """Load YAML configuration from file.
        
        Args:
            config_path: Path to the YAML config file
            
        Returns:
            Dict containing configuration parameters
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file has invalid YAML
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        with open(config_path, 'r') as file:
            try:
                config = yaml.safe_load(file)
                return config or {}
            except yaml.YAMLError as e:
                self._logger.error(f"Error parsing YAML configuration: {e}")
                raise

    def parse_arguments(self) -> argparse.Namespace:
        """Parse command line arguments.
        
        Returns:
            Parsed arguments
        """
        default_config = self._get_default_config_path()
        
        parser = argparse.ArgumentParser(description='Sensor data processing pipeline')
        parser.add_argument(
            '--config',
            dest='config_path',
            default=default_config,
            help='Path to the configuration file (default: %(default)s)'
        )
        parser.add_argument(
            '--project',
            dest='project_id',
            help='GCP project ID (overrides config file)'
        )
        parser.add_argument(
            '--region',
            dest='region',
            help='GCP region (overrides config file)'
        )
        parser.add_argument(
            '--job-name',
            dest='job_name',
            help='Dataflow job name (overrides config file)'
        )
        parser.add_argument(
            '--temp-location',
            dest='temp_location',
            help='GCS temp location (overrides config file)'
        )
        parser.add_argument(
            '--staging-location',
            dest='staging_location',
            help='GCS staging location (overrides config file)'
        )
        parser.add_argument(
            '--runner',
            dest='runner',
            default='DirectRunner',
            help='Pipeline runner (default: DirectRunner)'
        )
        return parser.parse_args()

    def _validate_gcp_environment(self) -> None:
        """Validate GCP environment variables and authentication.
        
        Raises:
            RuntimeError: If required GCP configuration is missing
        """
        if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
            self._logger.warning("GOOGLE_APPLICATION_CREDENTIALS environment variable not set. "
                          "Make sure you're authenticated with GCP.")
        
        required_vars = ['GOOGLE_CLOUD_PROJECT']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing_vars)}")

    def initialize_pipeline_environment(self, args: Optional[argparse.Namespace] = None) -> Dict[str, Any]:
        """Initialize the pipeline environment and load configuration.
        
        Args:
            args: Optional parsed arguments (if None, will parse from command line)
            
        Returns:
            Dict with complete configuration for pipeline
            
        Raises:
            Various exceptions for configuration or environment issues
        """
        if args is None:
            args = self.parse_arguments()
        
        self._validate_gcp_environment()
        
        try:
            config = self._load_yaml_config(args.config_path)
            self._logger.info(f"Loaded configuration from {args.config_path}")
        except Exception as e:
            self._logger.error(f"Failed to load configuration: {e}")
            raise
        return config
