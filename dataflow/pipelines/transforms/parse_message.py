import apache_beam as beam
from ..models import SensorReading, TelemetryBatch, ErrorMessage
from google.protobuf.timestamp_pb2 import Timestamp

class ParseMessage(beam.DoFn):
    """DoFn for parsing PubSub messages containing Protocol Buffer data."""
    
    def process(self, element):
        """Parses a Pub/Sub message into Protocol Buffer objects.

        Args:
            element: PubSubMessage, the raw message from Pub/Sub.

        Yields:
            SensorReading: Parsed sensor data object.
        """
        try:
            # Try to parse as protocol buffer
            if isinstance(element, bytes):
                data = element
            else:
                # If element has attributes (like a PubSub message), get data
                data = element.data if hasattr(element, 'data') else element
            
            # First attempt to parse as TelemetryBatch
            try:
                batch = TelemetryBatch()
                batch.ParseFromString(data)
                
                # Yield each sensor reading in the batch
                for reading in batch.readings:
                    yield reading
                    
            # If not a batch, try as single SensorReading
            except Exception:
                reading = SensorReading()
                reading.ParseFromString(data)
                yield reading
                
        except Exception as e:
            # Create error message
            error = ErrorMessage()
            error.error_type = ErrorMessage.ErrorType.PARSING_ERROR
            error.error_message = f"Failed to parse protobuf message: {str(e)}"
            
            # Add timestamp
            now = Timestamp()
            now.GetCurrentTime()
            error.timestamp.CopyFrom(now)
            
            # Include the original data for debugging
            if isinstance(element, bytes):
                error.original_data = element
            else:
                try:
                    error.original_data = element.data
                except:
                    error.original_data = bytes(str(element), 'utf-8')
            
            # Yield error record (can be collected in a separate output)
            yield beam.pvalue.TaggedOutput('errors', error)