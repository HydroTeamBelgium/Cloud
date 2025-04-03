"""Main entry point for the sensor data processing job."""

import argparse
import sys
import os
from typing import Dict, List, Any
from pathlib import Path

from common.logger import LoggerFactory
from dataflow.pipelines.pipeline import DataflowPipeline

logger_factory = LoggerFactory()
logger = logger_factory.get_logger(__name__, log_file="sensor_job.log")

def get_default_config_path() -> str:
    """Get the default configuration file path.
    
    Returns:
        str: Path to the default config file
    """
    # First check if config path is provided via environment variable
    if config_path := os.getenv('DATAFLOW_CONFIG_PATH'):
        return config_path
        
    # Otherwise use the default config in the dataflow directory
    base_dir = Path(__file__).resolve().parents[1]
    return str(base_dir / 'config.yaml')

def parse_arguments():
    """Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    default_config = get_default_config_path()
    
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
    return parser.parse_args()

def validate_gcp_environment():
    """Validate GCP environment variables and authentication.
    
    Raises:
        RuntimeError: If required GCP configuration is missing
    """
    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        logger.warning("GOOGLE_APPLICATION_CREDENTIALS environment variable not set. "
                      "Make sure you're authenticated with GCP.")
    
    # Check for minimum required environment variables
    required_vars = ['GOOGLE_CLOUD_PROJECT']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing_vars)}")

def main():
    """Main entry point for the sensor data processing job."""
    try:
        # Parse arguments
        args = parse_arguments()
        logger.info(f"Starting sensor data processing job with config: {args.config_path}")
        
        # Validate GCP environment
        validate_gcp_environment()
        
        # Verify config file exists
        if not os.path.exists(args.config_path):
            logger.error(f"Configuration file not found: {args.config_path}")
            sys.exit(1)
        
        # Create pipeline with config overrides from command line
        config_overrides = {
            key: value for key, value in vars(args).items()
            if key not in ['config_path'] and value is not None
        }
        
        # Initialize and run pipeline
        logger.info("Initializing pipeline with configuration...")
        pipeline = DataflowPipeline(
            config_path=args.config_path,
            config_overrides=config_overrides
        )
        
        logger.info("Submitting pipeline job to Dataflow...")
        pipeline.run()
        
        logger.info("Pipeline job submitted successfully")
        
    except Exception as e:
        logger.exception(f"Error running sensor data processing job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()