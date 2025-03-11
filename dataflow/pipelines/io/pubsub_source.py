from .base_source import BaseSource
import apache_beam as beam

class PubSubSource(BaseSource):
    def __init__(self, subscriptions):
        """Initializes the PubSubSource with a list of subscriptions.

        Args:
            subscriptions: List of str, Pub/Sub subscription paths (e.g., 'projects/myproject/subscriptions/sub1').
        """
        self.subscriptions = subscriptions

    def read(self, pipeline):
        """Reads from multiple Pub/Sub subscriptions and returns a merged PCollection.

        Args:
            pipeline: The Beam pipeline object.

        Returns:
            beam.PCollection: Merged data from all subscriptions.
        """
        pass