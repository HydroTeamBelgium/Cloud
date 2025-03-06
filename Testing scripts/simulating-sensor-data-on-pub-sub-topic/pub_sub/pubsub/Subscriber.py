from google.cloud import pubsub_v1

class Subscriber:
    """
    A class to handle subscribing to a Pub/Sub topic and processing messages.
    """

    def __init__(self, project_id: str, subscription_name: str, protobuf_class):
        """
        Initialize the Subscriber.
        Args:
            project_id (str): The Google Cloud project ID.
            subscription_name (str): The name of the subscription to listen to.
            protobuf_class : The Protobuf message class
        """

        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_name)
        

    def subscribe(self, callback: callable):
        """
        Start listening to messages from the subscription.
        Args:
            callback (callable): A function to process incoming messages.
            The function should accept a single argument (the message).
        """
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=callback)
        print(f"Listening for messages on {self.subscription_path}")
        streaming_pull_future.result() 

    def acknowledge(self, message):
        """
        Acknowledge a message after it has been processed.
        Args: 
            message: The message to be acknowledged.
        """
        self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[message.ack_id])
        print(f"Acknowledged message is: {message.message_id}")

    def pull_messages(self, max_messages: int) -> list:
        """
        Pull messages from the subscription.
        Args: 
            max_messages (int): The maximum number of messages to pull.
        Returns:
            list: A list of messages.
        """
        response = self.subscriber.pull(request={"subscription": self.subscription_path, "max_messages": max_messages})
        messages = response.received_messages
        print(f"Pulled {len(messages)} messages.")
        return messages

    def close(self):
        """
        Close the subscriber client and release resources.
        """
        self.subscriber.close()
        
        print("Subscriber client closed.")

    def deserialize_protobuf(self, binary_data):
        """
        Deserialize binary data into a Protobuf object.
        Args: 
            binary_data (bytes): The binary data to deserialize.
        Returns:
            The deserialized Protobuf object.
        """
        protobuf_message = self.protobuf_class()
        protobuf_message.ParseFromString(binary_data)
        return protobuf_message
    
    def message_size(self, messages):
        """"
        Calculate the size of the binary message"
        Args : 
            messages : Messages pulled from the subscription
        """ 
        print(f"Pulled {len(messages)} messages.")