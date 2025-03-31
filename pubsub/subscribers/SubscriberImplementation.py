from google.cloud import pubsub_v1
import google.auth.exceptions
import google.api_core.exceptions
from typing import Optional, Callable
from collections import namedtuple
from common.logger import get_logger, set_console_log_level  
from typing import Tuple, Type

MessageStats = namedtuple('MessageStats', ['count', 'total_size'])


class Subscriber:

    default_timeout = 60

    def __init__(self, project_id: str, subscription_name: str, protobuf_class: Type, platform: str = "GCP", service_account_path: Optional[str] = None):

        self.project_id = project_id
        self.subscription_name = subscription_name
        self.protobuf_class = protobuf_class
        self.logger = get_logger(__name__)  

        if platform == "GCP":
            self.logger.info(f"Platform {platform} selected. Using Google Cloud Platform.") 
            try:
                if service_account_path:
                    self.subscriber = pubsub_v1.SubscriberClient.from_service_account_file(service_account_path)
                else:
                    self.subscriber = pubsub_v1.SubscriberClient()
                
                self.subscription_path = self.subscriber.subscription_path(project_id, subscription_name)
            
            except google.auth.exceptions.DefaultCredentialsError as e:
                self.logger.error("Error: Could not authenticate using default credentials.")
                raise
            except google.api_core.exceptions.NotFound as e:
                self.logger.error(f"Error: Project or subscription not found. Project: {self.project_id}, Subscription: {self.subscription_name}") 
                raise
            except ValueError as e:
                self.logger.error(f"Error: Invalid project ID or subscription name. {e}") 
                raise
            except Exception as e:
                self.logger.error(f"An unexpected error occurred: {e}") 
                raise
        else:
            self.logger.info(f"Platform {platform} selected. Additional platform handling can be implemented later.")
            raise NotImplementedError("Only GCP is supported currently.")


    def subscribe(self, callback: Callable[[pubsub_v1.subscriber.message.Message], None]):
        try:
            streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=callback)
            self.logger.info(f"Listening for messages on {self.subscription_path}")
            streaming_pull_future.result() 
        except Exception as e:
            self.logger.error(f"Error during subscription: {e}") 
            raise 


    def __acknowledge(self,  message: pubsub_v1.subscriber.message.Message):
        try:
            self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[message.ack_id])
            self.logger.info(f"Acknowledged message: {message.message_id}")
        except Exception as e:
            self.logger.error(f"Error acknowledging message {message.message_id}: {e}")


    def receive(self, max_messages: int = 1, timeout: float = default_timeout):
        try:
            response = self.subscriber.pull(request={
                "subscription": self.subscription_path,
                "max_messages": max_messages,
                "timeout": timeout
            })
            messages = response.received_messages

            if not messages:
                self.logger.info(f"No messages received within {timeout} seconds.") 
                return [], MessageStats(0, 0)

            self.logger.info(f"Received {len(messages)} messages.") 
            deserialized_messages = []
            for message in messages:
                deserialized_message = self.__deserialize_protobuf(message.data)
                deserialized_messages.append(deserialized_message)
                self.__acknowledge(message)

            return deserialized_messages, self.__message_size(messages)

        except Exception as e:
            self.logger.error(f"Error during message pull: {e}") 
            return [], MessageStats(0, 0)


    def close(self):
        self.subscriber.close()
        self.logger.info("Subscriber client closed.")


    def __deserialize_protobuf(self, binary_data: bytes):
        protobuf_message = self.protobuf_class()
        protobuf_message.ParseFromString(binary_data)
        return protobuf_message
    

    def __message_size(self, messages: Tuple):
        if not messages:
            return MessageStats(0, 0)
    
        total_size = sum(len(message.data) for message in messages)
        return MessageStats(len(messages), total_size)
    