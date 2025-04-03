import sys
from .init import (
    initialize_pipeline_environment, 
    parse_arguments
)
from .pipeline import SensorDataPipeline
from common.logger import LoggerFactory

def main():
    """Main entry point for the sensor data processing pipeline.
    
    This function orchestrates the following steps:
    1. Initialize the pipeline environment (config, GCP auth)
    2. Create and configure the pipeline
    3. Run the pipeline
    """
    logger_factory = LoggerFactory()
    logger = logger_factory.get_logger(__name__)
    
    try:
        args = parse_arguments()
        
        logger.info("Initializing pipeline environment...")
        config = initialize_pipeline_environment(args)
        
        logger.info("Creating pipeline with configuration...")
        pipeline = SensorDataPipeline(config=config)
        
        logger.info("Submitting pipeline job...")
        pipeline.run()
        
        logger.info("Pipeline job submitted successfully")
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Error running sensor data processing pipeline: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
