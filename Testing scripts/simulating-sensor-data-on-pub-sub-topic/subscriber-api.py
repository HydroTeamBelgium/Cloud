from google.cloud import pubsub_v1

class Subscriber:
 

    def __init__(self, project_id: str, subscription_name: str):
        """
        arguments:
            project_id (str): project ID.
            subscription_name (str): The name of the subscription to listen to.
        """
        pass

    def subscribe(self, callback: callable):
        """
        arguments:
            callback (callable): A function to process incoming messages.
        """
        pass

    def acknowledge(self, message):
        """
        Acknowledge a message after it has been processed successfully.
        arguments:  message: The Pub/Sub message to acknowledge.
        """
        pass

    def pull_messages(self, max_messages: int) -> list:
        """
        Pull messages from the subscription.
        arguments: max_messages (int): The maximum number of messages to pull.
        Returns: A list of messages.
        """
        pass

    def close(self):
        """
        Close the subscriber client 
        """
        pass