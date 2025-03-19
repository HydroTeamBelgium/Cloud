import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
import logging
from typing import Dict, List, Any, Optional, Union

from dataflow.pipelines.io import (
    BaseSource, BaseSink, 
    PubSubSource, SqlSink, BigQuerySink
)
from dataflow.pipelines.transforms import SensorDataProcessingTransform
from typing import Type

logger = logging.getLogger(__name__)

class SensorDataPipeline:
    """Main pipeline orchestration class for racing car telemetry Dataflow processing.
    
    This class manages the creation and execution of an Apache Beam pipeline
    to process sensor data from racing cars, transforming and storing it
    in various output destinations.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the racing car telemetry pipeline.
        
        Args:
            config: Configuration dictionary with pipeline settings.
                   If not provided, will attempt to load from a default location.
        """
        self.config = config or self._load_default_config()
        
        self.project_id = self.config.get('project_id')
        self.region = self.config.get('region', 'us-central1')
        self.job_name = self.config.get('job_name', 'racing-car-telemetry')
        
        # Pub/Sub configuration
        self.pubsub_config = self.config.get('pubsub', {})
        self.topics = self.pubsub_config.get('topics', [])
        self.subscriptions = self.pubsub_config.get('subscriptions', [])
        
        # Database configuration
        self.sql_config = self.config.get('sql', {})
        self.table_mapping = self.config.get('table_mapping', {})
        
        # BigQuery configuration (if used)
        self.bigquery_config = self.config.get('bigquery', {})
        
        # Sensor configuration
        self.sensor_config = self.config.get('sensors', {})
        self.component_map = self.config.get('components', {})
        self.validation_config = self.config.get('validation', {})
        
        # Initialize components
        self.sources = []
        self.transforms = []
        self.sinks = []
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load configuration from default location.
        
        Returns:
            Dict containing configuration parameters.
        """
        # This would normally load from a configuration file or service
        # For now, return a minimal default configuration
        return {
            'project_id': 'my-racing-telemetry-project',
            'region': 'us-central1',
            'job_name': 'racing-car-telemetry',
            'pubsub': {
                'topics': ['projects/my-project/topics/sensor-data'],
                'subscriptions': []
            },
            'sql': {
                'connection_url': 'jdbc:mysql://localhost:3306/telemetry',
                'driver_class_name': 'com.mysql.jdbc.Driver',
                'username': 'user',
                'password': 'password'
            },
            'sensors': {
                'high_frequency': ['sensor1', 'sensor2'],
                'medium_frequency': ['sensor3', 'sensor4'],
                'low_frequency': ['sensor5']
            },
            'validation': {
                'min_value': -100,
                'max_value': 1000
            }
        }
    
    def create_sources(self) -> List[BaseSource]:
        """Create and configure data sources based on configuration.
        
        Returns:
            List of BaseSource objects configured for this pipeline.
        """
        sources = []
        
        # Add PubSub source if configured
        if self.topics or self.subscriptions:
            pubsub_source = PubSubSource(
                topics=self.topics,
                subscriptions=self.subscriptions,
                with_attributes=self.pubsub_config.get('with_attributes', False),
                id_label=self.pubsub_config.get('id_label'),
                timestamp_attribute=self.pubsub_config.get('timestamp_attribute')
            )
            sources.append(pubsub_source)
        
        # Additional sources could be added here based on configuration
        
        self.sources = sources
        return sources
    
    def create_transforms(self) -> List[beam.PTransform]:
        """Create and configure data transformations based on configuration.
        
        Returns:
            List of PTransform objects configured for this pipeline.
        """
        # Create sensor data processing transform
        sensor_transform = SensorDataProcessingTransform(
            component_map=self.component_map,
            validation_config=self.validation_config,
            metadata={'pipeline_id': self.job_name}
        )
        
        self.transforms = [sensor_transform]
        return self.transforms
    
    def create_sinks(self) -> List[BaseSink]:
        """Create and configure data sinks based on configuration.
        
        Returns:
            List of BaseSink objects configured for this pipeline.
        """
        sinks = []
        
        # Add SQL sink if configured
        if self.sql_config:
            sql_sink = SqlSink(
                connection_url=self.sql_config.get('connection_url'),
                driver_class_name=self.sql_config.get('driver_class_name'),
                table_name=self.sql_config.get('table_name')
            )
            sinks.append(sql_sink)
        
        # Add BigQuery sink if configured
        if self.bigquery_config:
            bigquery_sink = BigQuerySink(
                project=self.project_id,
                dataset=self.bigquery_config.get('dataset'),
                table=self.bigquery_config.get('table'),
                schema=self.bigquery_config.get('schema')
            )
            sinks.append(bigquery_sink)
        
        self.sinks = sinks
        return sinks
    
    def create_pipeline_options(self) -> PipelineOptions:
        """Create pipeline options based on configuration.
        
        Returns:
            Configured PipelineOptions object for running the pipeline.
        """
        pipeline_args = [
            f'--project={self.project_id}',
            f'--region={self.region}',
            f'--job_name={self.job_name}',
        ]
        
        # Add staging and temp locations if they exist in config
        staging_location = self.config.get('staging_location')
        if staging_location:
            pipeline_args.append(f'--staging_location={staging_location}')
            
        temp_location = self.config.get('temp_location')
        if temp_location:
            pipeline_args.append(f'--temp_location={temp_location}')
            
        # Set runner (DirectRunner for local, DataflowRunner for cloud)
        runner = self.config.get('runner', 'DirectRunner')
        pipeline_args.append(f'--runner={runner}')
        
        # Create and configure PipelineOptions
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options.view_as(SetupOptions).save_main_session = True
        
        return pipeline_options
    
    def build_pipeline(self, pipeline):
        """Build the pipeline by connecting sources, transforms, and sinks.
        
        Args:
            pipeline: The beam.Pipeline object to build upon.
            
        Returns:
            The fully constructed pipeline.
        """
        # Create components if they don't exist
        if not self.sources:
            self.create_sources()
        if not self.transforms:
            self.create_transforms()
        if not self.sinks:
            self.create_sinks()
            
        # Read from sources
        if not self.sources:
            raise ValueError("No sources configured for the pipeline")
            
        # Read from each source
        source_collections = []
        for i, source in enumerate(self.sources):
            pcoll = source.read(pipeline)
            source_collections.append(pcoll)
            
        # Combine sources if multiple
        if len(source_collections) > 1:
            pcoll = source_collections | "CombineSources" >> beam.Flatten()
        else:
            pcoll = source_collections[0]
            
        # Apply transforms - handle transforms that return dictionaries
        # Apply the main sensor data processing transform
        if self.transforms:
            main_transform = self.transforms[0]
            transform_result = pcoll | "ProcessSensorData" >> main_transform
            
            # For the main transform, we know it returns a dict with 'output' and 'errors'
            if isinstance(transform_result, dict) and 'output' in transform_result:
                # Main output PColl for sinks
                output_pcoll = transform_result['output']
                
                # Handle errors (could write to error log or separate sink)
                if 'errors' in transform_result:
                    error_pcoll = transform_result['errors']
                    # Write errors to log sink or similar
                    _ = error_pcoll | "LogErrors" >> beam.Map(logger.error)
            else:
                # If transform doesn't return a dict, use it directly
                output_pcoll = transform_result
        else:
            output_pcoll = pcoll
            
        # Write to sinks
        for i, sink in enumerate(self.sinks):
            sink.write(output_pcoll)
            
        return pipeline
    
    def run(self):
        """Build and run the pipeline end to end.
        
        Returns:
            The result of the pipeline run.
        """
        # Create pipeline options
        options = self.create_pipeline_options()
        
        # Create and build pipeline
        with beam.Pipeline(options=options) as pipeline:
            self.build_pipeline(pipeline)
            
        logger.info("Pipeline execution completed")


# If module is run directly, execute the pipeline
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    pipeline = SensorDataPipeline()
    pipeline.run()