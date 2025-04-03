from abc import ABC, abstractmethod
import apache_beam as beam

class BaseSource(ABC):
    """Abstract base class for all data sources in the racing car telemetry system.
    
    A source is responsible for reading data from an external system and
    converting it into a PCollection for further processing.
    """
    
    @abstractmethod
    def read(self, pipeline):
        """Reads data from the source into a PCollection.

        Args:
            pipeline: beam.Pipeline, the pipeline to add the source to.

        Returns:
            beam.PCollection: The data read from the source.
        """
        pass
    
    @abstractmethod
    def configure(self, **kwargs):
        """Configure the source with source-specific parameters.
        
        Args:
            **kwargs: Configuration parameters specific to the source implementation.
            
        Returns:
            self: Returns the source instance for method chaining.
        """
        pass
    
    @abstractmethod
    def get_schema(self):
        """Returns the schema of the data produced by this source.
        
        Returns:
            schema: The schema object describing the data structure.
        """
        pass