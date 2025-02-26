from google.cloud import pubsub_v1

class Subscriber:
 

    def __init__(self, project_id: str, subscription_name: str):
        """
        arguments:
            project_id (str): project ID.
            subscription_name (str): The name of the subscription to listen to.
        """
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_name)
        pass

    def subscribe(self, callback: callable):
        """
        arguments:
            callback (callable): A function to process incoming messages.
        """
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=callback)
        print(f"Listening for messages on {self.subscription_path}...")
        streaming_pull_future.result()
        pass

    def acknowledge(self, message):
        """
        Acknowledge a message after it has been processed successfully.
        arguments:  message: The Pub/Sub message to acknowledge.
        """
        self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[message.ack_id])
        print(f"Acknowledged message: {message.message_id}")
        pass

    def pull_messages(self, max_messages: int) -> list:
        """
        Pull messages from the subscription.
        arguments: max_messages (int): The maximum number of messages to pull.
        Returns: A list of messages.
        """
        response = self.subscriber.pull(request={"subscription": self.subscription_path, "max_messages": max_messages})
        messages = response.received_messages
        print(f"Pulled {len(messages)} messages.")
        return messages
        pass

    def close(self):
        """
        Close the subscriber client 
        """
        self.subscriber.close()
        print("Subscriber client closed.")
        pass