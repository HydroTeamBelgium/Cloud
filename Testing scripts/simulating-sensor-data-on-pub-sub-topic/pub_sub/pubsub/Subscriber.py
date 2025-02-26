from google.cloud import pubsub_v1

class Subscriber:
    """
    A class to handle subscribing to a Pub/Sub topic and processing messages.
    """

    def __init__(self, project_id: str, subscription_name: str):
        """
        Initialize the Subscriber.
        Args:
            project_id (str): The Google Cloud project ID.
            subscription_name (str): The name of the subscription to listen to.
        """
        pass

    def subscribe(self, callback: callable):
        """
        Start listening to messages from the subscription.
        Args:
            callback (callable): A function to process incoming messages.
            The function should accept a single argument (the message).
        """
        pass

    def acknowledge(self, message):
        """
        Acknowledge a message after it has been processed.
        Args: 
            message: The Pub/Sub message to be acknowledged.
        """
        pass

    def pull_messages(self, max_messages: int) -> list:
        """
        Pull messages from the subscription.
        Args: 
            max_messages (int): The maximum number of messages to pull.
        Returns:
            list: A list of messages.
        """
        pass

    def close(self):
        """
        Close the subscriber client and release resources.
        """
        pass