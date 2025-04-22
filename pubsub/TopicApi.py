from google.cloud import pubsub_v1
import google.auth.exceptions
import google.api_core.exceptions
from google.protobuf.message import Message
from google.api_core.exceptions import DeadlineExceeded, ServiceUnavailable, InvalidArgument
from typing import Any, Optional, Iterable
from pubsub.publishers.Publisher import Publisher
from pubsub.subscribers.Subscriber import Subscriber
from queue import Queue
from common.logger import LoggerFactory
import logging
from typing import Type

class Topic:
    """
    Topic class that acts as an abstraction over a Pub/Sub topic.
    It wraps a Publisher, a Subscriber, and an internal queue.

    API on Notion : https://www.notion.so/Topic-API-1a0ed9807d5880168436c926af242b3d?pvs=4
    """

    def __init__(self, project_id: str, topic_name: str, subscription_name: str, protobuf_class: Type[Message], maxsize: int = 0):
        """
        Initializes the Topic class.

        Args:
            project_id (str): Google Cloud project ID.
            topic_name (str): The name of the topic.
            subscription_name (str): The name of the subscription.
            protobuf_class (Type[Message]): The Protobuf message class used to deserialize incoming messages.
            maxsize (int): Maximum size of the internal queue (0 means infinite).
        """
        pass

    def put(self, item: Any, block: bool = True, timeout: Optional[float] = None) -> None:
        """
        Adds an item to the queue.

        Args:
            item (Any): The item to be added to the queue.
            block (bool): Whether to block if the queue is full.
            timeout (float): How long to block before giving up.
        """
        pass

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Removes and returns an item from the queue.

        Args:
            block (bool): Whether to block if the queue is empty.
            timeout (float): How long to block before giving up.

        Returns:
            Any: The item removed from the queue.
        """
        pass

    def qsize(self) -> int:
        """
        Returns the approximate size of the queue.

        Returns:
            int: Number of items in the queue.
        """
        pass

    def empty(self) -> bool:
        """
        Returns True if the queue is empty, False otherwise.

        Returns:
            bool: True if empty, False if not.
        """
        pass

    def full(self) -> bool:
        """
        Returns True if the queue is full, False otherwise.

        Returns:
            bool: True if full, False if not.
        """
        pass

    def _preprocess(self, item: Any) -> Any:
        """
        Placeholder for preprocessing step. Currently returns the input unchanged.

        Args:
            item (Any): The input item.

        Returns:
            Any: The output item (same as input for now).
        """
        pass

    def check_and_publish(self) -> None:
        """
        Checks the queue for messages, applies preprocessing, and sends to the Publisher.
        """
        pass

    def check_subscriber_errors(self) -> None:
        """
        Checks the Subscriber for any error messages.
        """
        pass 