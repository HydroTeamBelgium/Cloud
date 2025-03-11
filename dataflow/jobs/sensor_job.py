"""Main entry point for the sensor data processing job."""

import argparse
import logging
import sys
import os
from typing import Dict, List, Any

from dataflow.pipelines.pipeline import DataflowPipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Sensor data processing pipeline')
    parser.add_argument(
        '--config',
        dest='config_path',
        default='config.yaml',
        help='Path to the configuration file'
    )
    return parser.parse_args()

def main():
    """Main entry point for the sensor data processing job."""
    try:
        args = parse_arguments()
        logger.info(f"Starting sensor data processing job with config: {args.config_path}")
        
        if not os.path.exists(args.config_path):
            logger.error(f"Configuration file not found: {args.config_path}")
            sys.exit(1)
        
        pipeline = DataflowPipeline(args.config_path)
        pipeline.run()
        
        logger.info("Pipeline job submitted successfully")
    except Exception as e:
        logger.exception(f"Error running sensor data processing job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()