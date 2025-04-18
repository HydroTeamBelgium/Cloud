import os
import yaml
import subprocess
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

from common.logger import LoggerFactory


class PipelineInitializer:
    """Class for initializing and configuring the sensor data pipeline in GCP Dataflow."""
    
    def __init__(self):
        """Initialize the PipelineInitializer with a logger."""
        logger_factory = LoggerFactory()
        self._logger = logger_factory.get_logger(__name__, log_file="sensor_pipeline.log")
    
    def _get_config_path(self) -> str:
        """Get the configuration file path. It will check for the DATAFLOW_CONFIG_PATH 
        environment variable first and if not found, it will use the default config file 
        in the dataflow directory.
        Returns:
            str: Path to the config file
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

    def _validate_gcp_environment(self) -> None:
        """Validate GCP environment variables and authentication.
        
        Raises:
            RuntimeError: If required GCP configuration is missing
        """
        if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
            self._logger.warning("GOOGLE_APPLICATION_CREDENTIALS environment variable not set. "
                        "Make sure you're authenticated with GCP.")
            try:
                subprocess.run(
                    ["gcloud", "auth", "list"], 
                    check=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE
                )
            except (subprocess.SubprocessError, FileNotFoundError):
                self._logger.error("No GCP authentication found. Please run 'gcloud auth login' or set GOOGLE_APPLICATION_CREDENTIALS.")
                raise RuntimeError("GCP authentication required")
        
        required_vars = ['GOOGLE_CLOUD_PROJECT']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing_vars)}")

    def _validate_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate the configuration for required Dataflow settings.
        
        Args:
            config: The loaded configuration
            
        Returns:
            List of warning messages for incomplete but non-fatal issues
        """
        warnings = []
        
        if not config.get('gcp'):
            raise ValueError("Missing 'gcp' section in configuration")
        
        gcp_config = config['gcp']
        required_gcp_fields = ['project', 'region', 'job_name']
        
        for field in required_gcp_fields:
            if not gcp_config.get(field):
                raise ValueError(f"Missing required GCP configuration field: {field}")
        
        if not gcp_config.get('temp_location'):
            warnings.append("No 'temp_location' specified in GCP config. Dataflow will use default.")
        
        if not gcp_config.get('staging_location'):
            warnings.append("No 'staging_location' specified in GCP config. Dataflow will use default.")
        
        if not config.get('pubsub') or not config['pubsub'].get('topics'):
            warnings.append("No PubSub topics configured. Pipeline may not have input sources.")
        
        if not config.get('sensors'):
            warnings.append("No sensors configured. Pipeline may not process any data.")
        
        if not config.get('sinks'):
            warnings.append("No sinks configured. Pipeline output will not be stored.")
        
        return warnings
    
    def _verify_storage_buckets(self, config: Dict[str, Any]) -> None:
        """Verify that required GCS buckets exist and are accessible.
        
        Args:
            config: The configuration dictionary
            
        Raises:
            RuntimeError: If required buckets don't exist or are not accessible
        """
        if not config.get('gcp'):
            return
        
        gcp_config = config['gcp']
        buckets_to_check = []
        
        if temp_location := gcp_config.get('temp_location'):
            if temp_location.startswith('gs://'):
                bucket_name = temp_location.split('/')[2]
                buckets_to_check.append(bucket_name)
        
        if staging_location := gcp_config.get('staging_location'):
            if staging_location.startswith('gs://'):
                bucket_name = staging_location.split('/')[2]
                buckets_to_check.append(bucket_name)
        
        if (config.get('sinks') and 
            config['sinks'].get('cloud_storage') and 
            config['sinks']['cloud_storage'].get('enabled', False)):
            
            cs_config = config['sinks']['cloud_storage']
            if bucket := cs_config.get('bucket'):
                buckets_to_check.append(bucket)
        
        if not buckets_to_check:
            self._logger.warning("No GCS buckets specified in configuration")
            return
        
        for bucket in buckets_to_check:
            try:
                subprocess.run(
                    ["gsutil", "ls", f"gs://{bucket}"],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                self._logger.info(f"GCS bucket {bucket} is accessible")
            except subprocess.SubprocessError:
                self._logger.error(f"GCS bucket {bucket} does not exist or is not accessible")
                raise RuntimeError(f"Required GCS bucket {bucket} not accessible")

    def _verify_pubsub_topics(self, config: Dict[str, Any]) -> None:
        """Verify that configured PubSub topics exist.
        
        Args:
            config: The configuration dictionary
        """
        if not config.get('pubsub') or not config['pubsub'].get('topics'):
            self._logger.warning("No PubSub topics configured")
            return
        
        project_id = config['gcp']['project']
        topics = config['pubsub']['topics']
        
        for topic in topics:
            try:
                if '/' in topic:
                    topic_path = topic
                else:
                    topic_path = f"projects/{project_id}/topics/{topic}"
                
                subprocess.run(
                    ["gcloud", "pubsub", "topics", "describe", topic_path, "--project", project_id],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                self._logger.info(f"PubSub topic {topic_path} exists and is accessible")
            except subprocess.SubprocessError:
                self._logger.warning(f"PubSub topic {topic_path} does not exist or is not accessible")

    def _verify_sql_sink(self, config: Dict[str, Any]) -> None:
        """Verify SQL sink configuration and connectivity.
        
        Args:
            config: The configuration dictionary
        """
        if not config.get('sinks') or not config['sinks'].get('sql'):
            self._logger.info("SQL sink not configured")
            return
        
        sql_config = config['sinks']['sql']
        
        if not sql_config.get('enabled', False):
            self._logger.info("SQL sink is disabled, skipping verification")
            return

        required_fields = ['instance', 'user', 'password', 'database']
        missing_fields = [field for field in required_fields if not sql_config.get(field)]
        
        if missing_fields:
            self._logger.warning(f"SQL sink configuration missing required fields: {', '.join(missing_fields)}")
            return
        
        if instance_connection_name := sql_config.get('instance_connection_name'):
            try:
                project_id = config['gcp']['project']
                
                instance_name = instance_connection_name.split(':')[-1]
                
                subprocess.run(
                    ["gcloud", "sql", "instances", "describe", instance_name, "--project", project_id],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                self._logger.info(f"Cloud SQL instance {instance_name} exists and is accessible")
                
                result = subprocess.run(
                    ["gcloud", "services", "list", "--project", project_id, "--filter", "name:sqladmin.googleapis.com", "--format", "json"],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                services = json.loads(result.stdout)
                if not services:
                    self._logger.warning("Cloud SQL Admin API is not enabled. Attempting to enable...")
                    subprocess.run(
                        ["gcloud", "services", "enable", "sqladmin.googleapis.com", "--project", project_id],
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    self._logger.info("Cloud SQL Admin API successfully enabled")
                
            except subprocess.SubprocessError:
                self._logger.warning(f"Cloud SQL instance verification failed. Check your SQL configuration.")
        else:
            self._logger.warning("SQL sink configured without instance_connection_name. Make sure the database is accessible.")
        
        self._logger.info("SQL sink configuration verified")

    def initialize_pipeline_environment(self) -> Dict[str, Any]:
        """Initialize the pipeline environment and load configuration.
            
        Returns:
            Dict with complete configuration from YAML file
            
        Raises:
            Various exceptions for configuration or environment issues
        """
        self._validate_gcp_environment()
        
        config_path = self._get_config_path()
        try:
            config = self._load_yaml_config(config_path)
            self._logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            self._logger.error(f"Failed to load configuration: {e}")
            raise
        
        warnings = self._validate_config(config)
        for warning in warnings:
            self._logger.warning(warning)
        
        self._verify_storage_buckets(config)
        self._verify_pubsub_topics(config)
        self._verify_sql_sink(config)
        
        os.environ['DATAFLOW_PROJECT'] = config['gcp']['project'] 
        os.environ['DATAFLOW_REGION'] = config['gcp']['region']
        if runner := config['gcp'].get('runner'):
            os.environ['DATAFLOW_RUNNER'] = runner
        
        self._logger.info("Pipeline environment successfully initialized")
        return config
