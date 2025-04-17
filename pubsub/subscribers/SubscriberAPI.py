from google.cloud import pubsub_v1
import google.auth.exceptions
import google.api_core.exceptions
from typing import Optional, Callable, Type, List, Tuple, Any
from collections import namedtuple
from common.logger import get_logger, set_console_log_level  



class Subscriber:
    """
    A class to handle subscribing to a Pub/Sub topic and processing messages.

    API Details on Notion : https://www.notion.so/subscriber-API-1a0ed9807d588058a01ecb3d2fec8802?pvs=4
    """

    def __init__(self, project_id: str, subscription_name: str, protobuf_class: Type, platform: str = "GCP", service_account_path: Optional[str] = None):
        """
        Initialize the Subscriber.
        Args:
            project_id (str): The Google Cloud project ID.
            subscription_name (str): The name of the subscription to listen to.
            protobuf_class (Type): The Protobuf message class for deserialization.
            platform (str): The platform to use (default is "GCP").
            service_account_path (Optional[str]): Path to the service account file (optional).
        """
        pass

    def subscribe(self, callback: Callable[[pubsub_v1.subscriber.message.Message], None]):
        """
        Start listening to messages from the subscription.
        Args:
            callback (callable): A function to process incoming messages. The function should accept a single argument (the message).
        """
        pass

    def __acknowledge(self,  message: pubsub_v1.subscriber.message.Message):
        """
        Private method to acknowledge a message after it has been processed and logs if any error is raised.
        Args:
            message: The message to be acknowledged.
        """
        pass

    def receive(self, max_messages: int = 1, timeout: float = 60):
        """
        Receive messages from the subscription. Uses default value for max_messages and timeout.
        Args:
            max_messages (int): Maximum number of messages to pull (default 1).
            timeout (float): Timeout in seconds (default 60).
        """
        pass

    def close(self)-> None:
        """
        Close the subscriber client and release resources.
        """
        pass

    def __deserialize_protobuf(self, binary_data: bytes):
        """
        Private method to deserialize binary data into a Protobuf object.
        Args:
            binary_data (bytes): The binary data to deserialize.
        Returns:
            The deserialized Protobuf object.
        """
        pass
    
    def __message_size(self, messages: Tuple):
        """
        Calculate the size of the binary messages.
        Args:
            messages: Messages pulled from the subscription.
        Returns:
            MessageStats: A named tuple containing the message count and total size.
        """
        pass