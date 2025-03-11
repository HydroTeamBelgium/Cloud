import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
import logging
from typing import Dict, List, Any, Optional

from dataflow.pipelines.io.sources import Source, MultiPubSubSource
from dataflow.pipelines.io.sinks import Sink, MySQLSink
from dataflow.pipelines.transforms.processors import SensorDataTransform
from dataflow.utils.config_utils import get_sensor_table_resolver
from common.config import load_config, get_section

logger = logging.getLogger(__name__)

class DataflowPipeline:
    """Main pipeline orchestration class for Dataflow."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the pipeline.
        
        Args:
            config_path: Optional path to the configuration file.
                         If not provided, will automatically determine the appropriate config.
        """
        logger.info(f"Initializing pipeline with config from {'default location' if config_path is None else config_path}")
        
        self.config = load_config(config_path)
        
        self.gcp_options = get_section('gcp', config_path)
        self.pubsub_topics = get_section('pubsub', config_path)['topics']
        self.sensor_config = get_section('sensors', config_path)
        self.mysql_config = get_section('mysql', config_path)
        
    def create_sources(self) -> List[Source]:
        """Create source components based on configuration.
        
        Returns:
            List of Source objects
        """
        return [MultiPubSubSource(topics=self.pubsub_topics)]
    
    def create_sinks(self) -> List[Sink]:
        """Create sink components based on configuration.
        
        Returns:
            List of Sink objects
        """
        table_resolver = get_sensor_table_resolver(self.sensor_config)
        return [MySQLSink(
            mysql_config=self.mysql_config,
            table_resolver=table_resolver
        )]
    
    def create_transforms(self) -> List[beam.PTransform]:
        """Create transform components based on configuration.
        
        Returns:
            List of PTransform objects
        """
        return [SensorDataTransform()]
    
    def create_pipeline_options(self) -> PipelineOptions:
        """Create pipeline options based on configuration.
        
        Returns:
            Configured PipelineOptions
        """
        pipeline_options = PipelineOptions()
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = self.gcp_options['project']
        google_cloud_options.region = self.gcp_options['region']
        google_cloud_options.job_name = self.gcp_options['job_name']
        google_cloud_options.staging_location = self.gcp_options['staging_location']
        google_cloud_options.temp_location = self.gcp_options['temp_location']
        pipeline_options.view_as(SetupOptions).save_main_session = True
        pipeline_options.view_as(GoogleCloudOptions).runner = self.gcp_options['runner']
        return pipeline_options
    
    def run(self):
        """Build and run the pipeline."""
        pipeline_options = self.create_pipeline_options()
        sources = self.create_sources()
        transforms = self.create_transforms()
        sinks = self.create_sinks()
        
        logger.info(f"Running pipeline with {len(sources)} sources, "
                   f"{len(transforms)} transforms, and {len(sinks)} sinks")
        
        with beam.Pipeline(options=pipeline_options) as p:
            collections = [source.read(p, f"Source_{i}") 
                          for i, source in enumerate(sources)]
            
            if len(collections) > 1:
                pcoll = (collections | "FlattenSources" >> beam.Flatten())
            else:
                pcoll = collections[0]
            
            for i, transform in enumerate(transforms):
                pcoll = pcoll | f"Transform_{i}" >> transform
            
            for i, sink in enumerate(sinks):
                sink.write(pcoll, f"Sink_{i}")