import apache_beam as beam
from ..models.sensor_data import SensorData

class ParseMessage(beam.DoFn):
    def process(self, element):
        """Parses a Pub/Sub message into a SensorData object.

        Args:
            element: PubSubMessage, the raw message from Pub/Sub.

        Yields:
            SensorData: Parsed sensor data object.
        """
        pass