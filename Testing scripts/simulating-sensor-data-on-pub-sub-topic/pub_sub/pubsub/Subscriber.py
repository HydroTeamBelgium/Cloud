from google.cloud import pubsub_v1
import google.auth.exceptions
import google.api_core.exceptions

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
            protobuf_class : The Protobuf message class
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
            
            except google.auth.exceptions.DefaultCredentialsError: 
                print("Error: Could not authenticate using default credentials.")
                raise e
            except google.api_core.exceptions.NotFound:
                print(f"Error: Project or subscription not found. Project: {self.project_id}, Subscription: {self.subscription_name}")
                raise e
            except ValueError:
                print("Error: Invalid project ID or subscription name.")
                raise e
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                raise e
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
            raise e
           


    def __acknowledge(self, message):
        """
        Private method to acknowledge a message after it has been processed.
        Args:
            message: The message to be acknowledged.
        """
        self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[message.ack_id])
        print(f"Acknowledged message is: {message.message_id}")


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
                "max_messages": max_messages
            })
            messages = response.received_messages

            if not messages:
                print(f"No messages received within {timeout} seconds.")
                return [], 0

            print(f"Received {len(messages)} messages.")
            for message in messages:
                self.__deserialize_protobuf(message.data) 
                self.__acknowledge(message)

            return messages, self.__message_size(messages)

        except Exception as e:
            print(f"Error during message pull: {e}")
            return [], 0


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
        Private method to calculate the size of the binary message.
        Args:
            messages: Messages pulled from the subscription.
        Returns:
            tuple: A tuple containing the message count and size.
        """
        total_size = sum(len(message.data) for message in messages)
        return len(messages), total_size