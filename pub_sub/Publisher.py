from google.cloud import pubsub_v1
import os


class Publisher:
    def __init__(self, project_id: str) -> None:
        '''Initialize Publisher class with the project_id'''
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()

        # List topics in the project
        self.topics = list(self.publisher.list_topics(request={"project": f"projects/{project_id}"}))

    def try_authenticate_google_cloud(self, service_account_path: str) -> bool:
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

    def get_topic_id(self, topic_name: str) -> str:
        return topic_name
    
    def get_topic_path(self, project_id: str, topic_name: str) -> str:
        return self.publisher.topic_path(project_id, topic_name)
    
    def create_proto_message(self, fields) -> bytes:
        """Converts a message with multiple fields to a message of bytes"""
        msg = str
        #msg = message_pb2.Sensor_data(fields)
        return msg.SerializeToString()

    def publish_message(self, topic_path: str, message: bytes) -> None:
        return
    
    def publish_message_to_cloud(self, fields) -> bool:
        if self.try_authenticate_google_cloud("service_account_path"):
            msg = self.create_proto_message(fields)
            self.publish_message(self.get_topic_path, msg)
            return True
        else:
            return False

