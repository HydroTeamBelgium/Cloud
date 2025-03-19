import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from .base_source import BaseSource
from typing import Dict, List, Optional, Union, Any

from ..models import SensorReading, TelemetryBatch

class PubSubSource(BaseSource):
    """Implementation of BaseSource for reading data from Google Cloud Pub/Sub.
    
    This source reads messages from one or more Pub/Sub topics or subscriptions
    and converts them into a PCollection for further processing.
    """
    
    def __init__(self, 
                 topics: Optional[List[str]] = None, 
                 subscriptions: Optional[List[str]] = None,
                 with_attributes: bool = False, 
                 id_label: Optional[str] = None,
                 timestamp_attribute: Optional[str] = None):
        """Initialize the PubSub source.
        
        Args:
            topics: List of Pub/Sub topic paths (e.g., 'projects/myproject/topics/topic1').
            subscriptions: List of Pub/Sub subscription paths (e.g., 'projects/myproject/subscriptions/sub1').
            with_attributes: Whether to include the Pub/Sub message attributes.
            id_label: If not None, the message ID will be stored in this attribute.
            timestamp_attribute: If not None, the timestamp will be extracted from this attribute.
        """
        self.topics = topics or []
        self.subscriptions = subscriptions or []
        self.with_attributes = with_attributes
        self.id_label = id_label
        self.timestamp_attribute = timestamp_attribute
        self._schema = None
    
    def configure(self, **kwargs):
        """Configure the PubSub source with specific parameters.
        
        Args:
            **kwargs: Configuration parameters which may include:
                - topics: List of topic paths
                - subscriptions: List of subscription paths
                - with_attributes: Whether to include message attributes
                - id_label: Attribute name for message ID
                - timestamp_attribute: Attribute name for timestamp
                - schema: Schema of the expected message format
            
        Returns:
            self: Returns the source instance for method chaining.
        """
        # Update configuration with provided values
        for key, value in kwargs.items():
            if key == 'topics' and isinstance(value, list):
                self.topics = value
            elif key == 'subscriptions' and isinstance(value, list):
                self.subscriptions = value
            elif key == 'with_attributes' and isinstance(value, bool):
                self.with_attributes = value
            elif key == 'id_label':
                self.id_label = value
            elif key == 'timestamp_attribute':
                self.timestamp_attribute = value
            elif key == 'schema':
                self._schema = value
                
        return self
    
    def get_schema(self):
        """Get the schema of the data this source produces.
        
        Returns:
            The schema definition, if available.
        """
        return self._schema
    
    def read(self, pipeline):
        """Read data from the configured PubSub topics or subscriptions.
        
        Args:
            pipeline: The beam.Pipeline to which this source will be applied.
            
        Returns:
            PCollection: A PCollection containing the parsed data.
            
        Raises:
            ValueError: If neither topics nor subscriptions are specified.
        """
        if not self.topics and not self.subscriptions:
            raise ValueError("At least one topic or subscription must be specified")
            
        # Configure read transform parameters
        read_params = {}
        
        if self.topics:
            read_params['topic'] = self.topics[0]  # Currently only supports a single topic
        elif self.subscriptions:
            read_params['subscription'] = self.subscriptions[0]  # Currently only supports a single subscription
            
        if self.id_label:
            read_params['id_label'] = self.id_label
            
        if self.timestamp_attribute:
            read_params['timestamp_attribute'] = self.timestamp_attribute
            
        # Read from PubSub
        pcoll = pipeline | "ReadFromPubSub" >> ReadFromPubSub(**read_params)
        
        # Parse the protobuf messages
        parsed_pcoll = pcoll | "ParseProtoMessages" >> beam.ParDo(ParseProtoMessage())
        
        return parsed_pcoll