from abc import ABC, abstractmethod
import apache_beam as beam

class BaseSink(ABC):
    """Abstract base class for all data sinks in the racing car telemetry system.
    
    A sink is responsible for writing data from a PCollection to a destination
    such as a database, storage system, or another messaging system.
    """
    
    @abstractmethod
    def write(self, pcollection):
        """Writes data from a PCollection to the sink.

        Args:
            pcollection: beam.PCollection, the data to write.

        Returns:
            beam.PCollection or None: Depending on implementation, may return a result PCollection.
        """
        pass
    
    @abstractmethod
    def configure(self, **kwargs):
        """Configure the sink with destination-specific parameters.
        
        Args:
            **kwargs: Configuration parameters specific to the sink implementation.
            
        Returns:
            self: Returns the sink instance for method chaining.
        """
        pass
    
    @abstractmethod
    def validate_schema(self, data_schema):
        """Validates that the incoming data schema is compatible with the sink.
        
        Args:
            data_schema: Schema of the incoming data.
            
        Returns:
            bool: True if the schema is valid for this sink, False otherwise.
        """
        pass