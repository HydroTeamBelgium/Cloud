from google.cloud import pubsub_v1
import os

# Define Publisher class to interact with Google Cloud Pub/Sub
class Publisher:
    def __init__(self, project_id: str) -> None:
        '''Initialize Publisher class with the project_id'''
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()

        # List topics in the project
        self.topics = list(self.publisher.list_topics(request={"project": f"projects/{project_id}"}))

    def authenticate(self, service_account_path: str) -> bool:
        """Authenticates using the Service Account JSON key."""
        try:
            # Set environment variable for authentication
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

            # Verify authentication
            self.publisher = pubsub_v1.PublisherClient()
            project_path = f"projects/{self.project_id}"
            
            print(f"Authentication successful for project: {project_path}")
            return True
        except Exception as e:
            print(f"Error during authentication: {e}")
            return False


    def get_allTopicInProject(self) -> list:
        return self.topics.clone()

    def _get_topic_id(self, topic_id: str) -> str:
        """Gets the right topic ID asked by the user from the list of topics"""
        try:
            for topic in self.topics:
                if topic == topic_id:
                    return topic_id
        except Exception as e:
            print(f"Topic ID not found: {e}")
    
    
    def _get_topic_path(self, topic_id: str) -> str:
        """Returns the full path of the topic within the project ID"""
        return self.publisher.topic_path(self.project_id, self._get_topic_id(topic_id))
    
    def _create_proto_message(self, fields) -> bytes:
        """Converts a message with multiple fields to a message of bytes"""
        try:
            msg = str
            #msg = message_pb2.Sensor_data(fields)
            return msg.SerializeToString()
        except Exception as e:
            print(f"Not able to convert fields to proto format (bytes): {e}")

    def _publish_message(self, topic_path: str, message: bytes) -> None:
        """Publishes the message to the specified topic"""
        try:
            # Publish the message and get a future to track the message delivery
            future = self.publisher.publish(topic_path, message)
            print(f"Message published to {topic_path}, message ID: {future.result()}")
        except Exception as e:
            print(f"Failed to publish message: {e}")
    
    def publish_message_to_cloud(self, topic_id: str,  fields) -> None:
        """Publishes a message to a topic, authenticates itself first before sending the message"""
        self.authenticate("service_account_path")
        msg = self._create_proto_message(fields)
        self._publish_message(self._get_topic_path(topic_id), msg)


