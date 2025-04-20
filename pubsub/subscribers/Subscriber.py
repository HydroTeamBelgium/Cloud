from google.cloud import pubsub_v1
import google.auth.exceptions
import google.api_core.exceptions
from typing import Dict, Optional, Callable, Any
from common.logger import LoggerFactory
from typing import Tuple, Type
from common.config import get_section
from google.protobuf.message import Message
from google.api_core.exceptions import DeadlineExceeded, ServiceUnavailable, InvalidArgument
import numpy as np
import logging

from pubsub.subscribers import MessageStats










class Subscriber:
    """
    A class to handle subscribing to a Pub/Sub topic and processing messages.

    API Details on Notion : https://www.notion.so/subscriber-API-1a0ed9807d588058a01ecb3d2fec8802?pvs=4
    """
    
    _default_timeout = int
    _pubsub_config= Dict[str,Any]
    _project_id: str
    _subscription_name: str
    _protobuf_class: Type[Message]
    _logger: logging.Logger
    _subscriber: pubsub_v1.SubscriberClient
    _subscription_path: str

    def __init__(self, project_id: str, subscription_name: str, protobuf_class: Type[Message], platform: str = "GCP", service_account_path: Optional[str] = None):

        """
        Initialize the Subscriber.
        Args:
            project_id (str): The Google Cloud project ID.
            subscription_name (str): The name of the subscription to listen to.
            protobuf_class (Type[Message]): The Protobuf message class used to deserialize incoming messages.
            platform (str): The platform to use (default is "GCP").
            service_account_path (Optional[str]): Path to a service account key file.
                If provided, this will be used for authentication (usually for local development).
                If not provided, the client will use Application Default Credentials.
        """     
        self._pubsub_config = get_section("pubsub")
        self._default_timeout = self._pubsub_config["subscriber"]["timeout"]

        self._project_id = project_id
        self._subscription_name = subscription_name
        self._protobuf_class = protobuf_class
        self._logger = LoggerFactory().get_logger(__name__)

        if platform == self._pubsub_config["platforms"]["default"]:
            self._logger.info(f"Platform {platform} selected. Using Google Cloud Platform.")
            try:
                if service_account_path:
                    self.subscriber = pubsub_v1.SubscriberClient.from_service_account_file(service_account_path)
                else:
                    self.subscriber = pubsub_v1.SubscriberClient()
                
                self.subscription_path = self.subscriber.subscription_path(project_id, subscription_name)
            
            except google.auth.exceptions.DefaultCredentialsError as e:
                self._logger.error("Authentication failed using Application Default Credentials." "No service account file was used.")
                # Uses Google's Application Default Credentials automatically provided by the environment (e.g. gcloud login ) when no service account is provided.
                raise
            except google.api_core.exceptions.NotFound as e:
                self._logger.error(f"Error: Project or subscription not found. Project: {self.project_id}, Subscription: {self.subscription_name}") 
                raise
            except ValueError as e:
                self._logger.error(f"Error: Invalid project ID or subscription name. {e}") 
                raise
            except Exception as e:
                self._logger.error(f"An unexpected error occurred: {e}") 
                raise
        else:
            self._logger.error(f"Unsupported platform: {platform}. Additional platform handling not implemented.")   
            raise NotImplementedError("Only GCP is supported currently.")


    def subscribe(self, callback: Callable[[pubsub_v1.subscriber.message.Message], None]):
       
        """
        Start listening to messages from the subscription.
        Args:
            callback (callable): A function to process incoming messages. The function should accept a single argument (the message).
        """

        try:
            streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=callback)
            self._logger.info(f"Listening for messages on {self.subscription_path}")
            streaming_pull_future.result() 
            """
            streaming_pull_future.result() ensures the subscriber stays active and continues listening for incoming messages.
            when subscribe() is called, it starts a background thread but it returns immediately. 
            '.result' is there so that the thread does not end unless there is something wrong
            """
        except Exception as e:
            self._logger.error(f"Error during subscription: {e}") 
            raise 


    def _acknowledge(self,  message: pubsub_v1.subscriber.message.Message):
        
        """
        Private method to acknowledge a message after it has been processed and logs if any error is raised.
        Args:
            message: The message to be acknowledged.
        """
        
        try:
            self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[message.ack_id])
            self._logger.info(f"Acknowledged message: {message.message_id}")
        except Exception as e:
            self._logger.error(f"Error acknowledging message {message.message_id}: {e}")


    def receive(self, max_messages: int = 1, timeout: float = default_timeout):
        
        """
        Receive messages from the subscription. Uses default value for max_messages and timeout.
        Args:
            max_messages (int): Maximum number of messages to pull (default 1).
            timeout (float): Timeout in seconds (default 60).
        """
        
        try:
            response = self._subscriber.pull(request={
                "subscription": self.subscription_path,
                "max_messages": max_messages,
                "timeout": timeout
            })
            messages = response.received_messages

            if not messages:
                self._logger.info(f"No messages received within {timeout} seconds.") 
                return np.empty(0, dtype=object), MessageStats(count=0, total_size=0)
            
        
            deserialized_messages = np.empty(len(messages), dtype=object)

            for i, message in enumerate(messages):
                deserialized_message = self._deserialize_protobuf(message.data)
                deserialized_messages[i] = deserialized_message
                self._acknowledge(message)

            return deserialized_messages, self.__message_size(messages)    

        except DeadlineExceeded as e:
            self._logger.warning(f"Pull request timed out: {e}")
        except ServiceUnavailable as e:
            self._logger.error(f"Pub/Sub service unavailable: {e}")
        except InvalidArgument as e:
            self._logger.error(f"Invalid pull request: {e}")
        except Exception as e:
            self._logger.error(f"Unexpected error during message pull: {e}")
            
        return np.empty(0, dtype=object), MessageStats(count=0, total_size=0)


    def close(self):
        
        """
        Close the subscriber client and release resources.
        """        

        self.subscriber.close()
        self._logger.info("Subscriber client closed.")


    def _deserialize_protobuf(self, binary_data: bytes):
       
        """
        Private method to deserialize binary data into a Protobuf object.
        Args:
            binary_data (bytes): The binary data to deserialize.
        Returns:
            The deserialized Protobuf object.
        """            
        try:
            protobuf_message = self._protobuf_class()
            protobuf_message.ParseFromString(binary_data)
            self._logger.debug(f"Successfully deserialized message.")
            return protobuf_message
        except Exception as e:
            self._logger.error(f"Failed to deserialize protobuf message: {e}")
            raise
    

    def __message_size(self, messages: Tuple):
        
        """
        Calculate the size of the binary messages.
        Args:
            messages: Messages pulled from the subscription.
        Returns:
            MessageStats: A named tuple containing the message count and total size.
        """        
        
        if not messages:
            return MessageStats(0, 0)
    
        total_size = sum(len(message.data) for message in messages)
        return MessageStats(count=len(messages), total_size=total_size)
    