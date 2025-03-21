from google.cloud import pubsub_v1
import google.auth.exceptions
import google.api_core.exceptions
from typing import Optional, Callable
from collections import namedtuple
import logging

MessageStats = namedtuple('MessageStats', ['count', 'total_size'])

class Subscriber:
    """
    A class to handle subscribing to a Pub/Sub topic and processing messages.
    """

    def __init__(self, project_id: str, subscription_name: str, protobuf_class, platform: str = "GCP", service_account_path: Optional[str] = None):
        """
         Initialize the Subscriber.
        Args:
            project_id (str): The Google Cloud project ID.
            subscription_name (str): The name of the subscription to listen to.
            protobuf_class: The Protobuf message class.
            platform (str): The platform to use (default is "GCP").
            service_account_path (Optional[str]): Path to the service account file (optional).
        """
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.protobuf_class = protobuf_class

        if platform == "GCP":
            print(f"Platform {platform} selected. Using Google Cloud Platform.")
            try:
                if service_account_path:
                    self.subscriber = pubsub_v1.SubscriberClient.from_service_account_file(service_account_path)
                else:
                    self.subscriber = pubsub_v1.SubscriberClient()
                
                self.subscription_path = self.subscriber.subscription_path(project_id, subscription_name)
            
            except google.auth.exceptions.DefaultCredentialsError as e:
                print("Error: Could not authenticate using default credentials.")
                raise
            except google.api_core.exceptions.NotFound as e:
                print(f"Error: Project or subscription not found. Project: {self.project_id}, Subscription: {self.subscription_name}")
                raise
            except ValueError as e:
                print(f"Error: Invalid project ID or subscription name. {e}")
                raise
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                raise
        else:
            print(f"Platform {platform} selected. Additional platform handling can be implemented later.")
            raise NotImplementedError("Only GCP is supported currently.")

        

    def subscribe(self, callback: Callable[[pubsub_v1.subscriber.message.Message], None]):
        """
        Start listening to messages from the subscription.
        Args:
            callback (callable): A function to process incoming messages. The function should accept a single argument (the message).
        """
        try:
            streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=callback)
            print(f"Listening for messages on {self.subscription_path}")
            streaming_pull_future.result() 
        except Exception as e:
            print(f"Error during subscription: {e}")
            raise 
           


    def __acknowledge(self, message):
        """
        Private method to acknowledge a message after it has been processed and logs if any error is raised.
        Args:
            message: The message to be acknowledged.
        """
        try:
            self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[message.ack_id])
            self.logger.info(f"Acknowledged message: {message.message_id}")
        except Exception as e:
            self.logger.error(f"Error acknowledging message {message.message_id}: {e}")

    def receive(self, max_messages=1, timeout=60):
        """
        Receive messages from the subscription. Uses default value for max_messages and timeout.
        Args:
            max_messages (int): Maximum number of messages to pull (default 1).
            timeout (int): Timeout in seconds (default 60).
        """
        try:
            response = self.subscriber.pull(request={
                "subscription": self.subscription_path,
                "max_messages": max_messages,
                "timeout": timeout
            })
            messages = response.received_messages

            if not messages:
                print(f"No messages received within {timeout} seconds.")
                return [], MessageStats(0, 0)

            print(f"Received {len(messages)} messages.")
            deserialized_messages = []
            for message in messages:
                deserialized_message = self.__deserialize_protobuf(message.data)
                deserialized_messages.append(deserialized_message)
                self.__acknowledge(message)

            return deserialized_messages, self.__message_size(messages)

        except Exception as e:
            print(f"Error during message pull: {e}")
            return [], MessageStats(0, 0)
        

    def close(self):
        """
        Close the subscriber client and release resources.
        """
        self.subscriber.close()
        
        print("Subscriber client closed.")

    def __deserialize_protobuf(self, binary_data):
        """
        Private method to deserialize binary data into a Protobuf object.
        Args:
            binary_data (bytes): The binary data to deserialize.
        Returns:
            The deserialized Protobuf object.
        """
        protobuf_message = self.protobuf_class()
        protobuf_message.ParseFromString(binary_data)
        return protobuf_message
    
    def __message_size(self, messages):
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
        return MessageStats(len(messages), total_size)