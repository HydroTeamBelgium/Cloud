from google.cloud import pubsub_v1
from common.logger import LoggerFactory
from google.cloud.pubsub_v1.types import Topic
from typing import Optional
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account
import message_pb2 

# Define Publisher class to interact with Google Cloud Pub/Sub
class Publisher:
    _project_id : str
    _service_account_path : str
    _platform : str
    _logger : LoggerFactory
    _topics : list[Topic]
    publisher : pubsub_v1.PublisherClient

    def __init__(self, platform  : str, project_id: str, service_account_path : str) -> None:
        '''Initialize Publisher class with the project_id'''
        #setup the logger
        _logger_factory = LoggerFactory()
        self._logger = _logger_factory.get_logger(__name__)

        self._project_id = project_id
        self._service_account_path = service_account_path
        self._platform = platform
        self.publisher = pubsub_v1.PublisherClient()

        # Makes a list of Topic objects within the project
        self._topics = list(self.publisher.list_topics(request={"project": f"projects/{project_id}"}))

    def authenticate(self, service_account_path: str) -> bool:
        """Authenticates using the Service Account JSON key."""
        if not self._project_id:
            self._logger.error("Project ID is not set.")
            return False
        if self._platform == "GCP":
            try:
                if service_account_path:
                    self._logger.info(f"Started authentication using a service account: {service_account_path}")
                    credentials = service_account.Credentials.from_service_account_file(service_account_path)
                    self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
                else :
                    self._logger.info(f"Started without service account")
                    self.publisher = pubsub_v1.PublisherClient()
                
                self._logger.debug(f"Authentication successful for project: projects/{self._project_id}")
                return True
            except FileNotFoundError as e:
                self._logger.error(f"Service account file not found: {e}")
                return False
            except DefaultCredentialsError as e:
                self._logger.error(f"Authentication failed due to default credentials issue: {e}")
                return False
            except ValueError as e:
                self._logger.error(f"Invalid service account file format: {e}")
                return False
            except Exception as e:
                # Catch any other unforeseen exceptions 
                self._logger.error(f"Unexpected error during authentication: {e}")
                return False
        else:
            self._logger.error(f"{self._platform} is currently not supported, use GCP")
            raise NotImplementedError("Only GCP is currently supported.")


    def get_all_topics_in_project(self) -> list[Topic]:
        """Returns all Topic objects existing in the project"""
        return self._topics.copy()

    def _check_topic_id_available(self, topic_id: str) -> bool:
        """Checks if the topic ID exists in the project"""
        try:
            for topic in self._topics:
                if topic.name == topic_id:
                    self._logger.info(f"Topic ID {topic.name} found")
                    return True
            return False
        except Exception as e:
            self._logger.warning(f"Topic ID not found: {e}")
            return False
    
    
    def _get_topic_path(self, topic_id: str) -> Optional[str]:
        """Returns the full path of the topic within the project ID"""

        #Checks if the topic ID exists in the project
        if (self._check_topic_id_available(topic_id)):
            self._logger.info(f"Topic path has been found")
            return self.publisher.topic_path(self._project_id, topic_id)

    
    def _create_proto_message(self, **kwargs) -> bytes:
        """
        Converts a message with multiple fields to a message of bytes
         The fields are defined as follows:
            key : value
        """
        try:
            # Create the instance of the Proto message 
            msg = message_pb2.SensorData()

            # Sets the fields of the proto message
            for key, value in kwargs.items():
                if hasattr(msg, key):  # Check if the key (field) exists in the proto message
                    setattr(msg, key, value)
                else:
                    self._logger.warning(f"Field {key} does not exist in SensorData message.")
            
            # Serialize the message to a byte stream
            return msg.SerializeToString()
        
        except Exception as e:
            self._logger.warning(f"Not able to convert fields to proto format (bytes): {e}")
            return b''  # Return empty byte string on failure

    def _publish_message(self, topic_path: str, message: bytes) -> None:
        """Publishes the message to the specified topic"""
        try:
            # Publish the message and get a future to track the message delivery
            future = self.publisher.publish(topic_path, message)
            self._logger.info(f"Message published to {topic_path}, message ID: {future.result()}")
        except Exception as e:
            self._logger.warning(f"Failed to publish message: {e}")
    
    def publish_message_to_cloud(self, topic_id: str,  **kwargs) -> None:
        """Publishes a message to a topic"""
        topic_path = self._get_topic_path(topic_id)
        if topic_path:
            msg = self._create_proto_message(**kwargs)
            self._publish_message(topic_path, msg)
        else:
            self._logger.warning(f"Topic {topic_id} does not exist.")


