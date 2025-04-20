from google.cloud import pubsub_v1
from common.config import ConfigFactory
from common.logger import LoggerFactory
from google.cloud.pubsub_v1.types import Topic
import google.auth.exceptions
import google.api_core.exceptions
from typing import Any, Dict, Optional, Type
from google.protobuf.message import Message
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account
import message_pb2 

# Define Publisher class to interact with Google Cloud Pub/Sub
class Publisher:

    """ Pulisher class to handle sending messages to a specified topic"""

    _project_id : str
    _service_account_path : str
    _platform : str
    _logger : LoggerFactory
    _topic_name : str
    _protobuf_class: Type[Message]
    _publisher : pubsub_v1.PublisherClient
    _pubsub_config= Dict[Any,Any]
    _publisher_path: str

    def __init__(self, project_id: str, topic_name: str, protobuf_class: Type[Message], platform: str = "GCP", service_account_path : Optional[str] = None) -> None:
        """
        Initialize Publisher class with the project_id
        
        Initializes a Publisher class for pub/sub with the project_id 
        
        args: 
            project_id (str): The Google Cloud project ID.
            topic_name (str): The name of the topic to listen to.
            protobuf_class (Type[Message]): The Protobuf message class used to deserialize incoming messages.
            platform (str): The platform to use (default is "GCP").
            service_account_path (Optional[str]): Path to a service account key file.
                If provided, this will be used for authentication (usually for local development).
                If not provided, the client will use Application Default Credentials.
        """
        #setup the logger
   
        self._logger = LoggerFactory().get_logger(__name__)
        self._pubsub_config = ConfigFactory().load_config() 
        
        self._project_id = project_id
        self._service_account_path = service_account_path
        self._platform = platform
        self._topic_name = topic_name
        self._protobuf_class = protobuf_class

        self._authenticate(platform, service_account_path)
    
    def _authenticate(self, platform:str,  service_account_path: str) -> bool:
        """
        Authenticate the subscriber to the topic. Should only be called on init 

        Args:
            platform (str): The platform to use (default is "GCP").
            service_account_path (Optional[str]): Path to a service account key file.
                If provided, this will be used for authentication (usually for local development).
                If not provided, the client will use Application Default Credentials.
        """
       
        if platform == self._pubsub_config["platforms"]["default"]:
            self._logger.info(f"Platform {platform} selected.")
            try:
                if service_account_path:
                    self._logger.info(f"Started authentication using a service account: {service_account_path}")
                    self._publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account_path)
                else:
                    self._logger.info(f"Started without service account")
                    self._publisher = pubsub_v1.PublisherClient()
                
                self._publisher_path = self._publisher.topic_path(self._project_id, self._topic_name)
            except FileNotFoundError as e:
                self._logger.error(f"Service account file not found: {e}")

            except ValueError as e:
                self._logger.error(f"Invalid service account file format: {e}")

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

    def _create_proto_message(self, **kwargs) -> bytes:
        """
        Converts a message with multiple fields to a message of bytes, used for the protobuff message
        
        args: /
        kwargs: this dictionary represents all the necessary fields in the protobuf message (see .get_required_keys_for_message())

        """
        try:
            # Create the instance of the Proto message 
            #msg = message_pb2.SensorData()

            # Sets the fields of the proto message
            for key, value in kwargs.items():
                if hasattr(self._protobuf_class, key):  # Check if the key (field) exists in the proto message
                    setattr(self._protobuf_class, key, value)
                else:
                    self._logger.warning(f"Field {key} does not exist in SensorData message.")
            
            # Serialize the message to a byte stream
            return self._protobuf_class.SerializeToString()
        
        except Exception as e:
            self._logger.warning(f"Not able to convert fields to proto format (bytes): {e}")
            return b''  # Return empty byte string on failure

    
    def get_required_keys_for_message(self) -> Dict[str, str]:
        """
        Returns the required keys for the protobuf message and the corresponding type

        args:/

        returns: a dictionary with as key the name of the field and as value the type in string format (so not encoded as type, but the name of the type)
        """
        return dict([(field_descriptor.name, self._pubsub_config["types_protobuf_translation"][field_descriptor.type]) \
                    for field_descriptor in self._protobuf_class.DESCRIPTOR.fields])


    def _publish_message(self, message: bytes) -> None:
        """
        Publishes the message to the specified topic, using the topic path and message
        
        args: 
            message: message converted to bytes

        returns: /
        
        Exceptions: logs an error level log if publishing to the cloud failed
        """
        try:
            # Publish the message and get a future to track the message delivery
            future = self.publisher.publish(self._topic_path, message)
            self._logger.info(f"Message published to {self._topic_path}, message ID: {future.result()}")
        except Exception as e:
            self._logger.warning(f"Failed to publish message: {e}")
    
    def publish_message_to_cloud(self, **kwargs) -> None:
        """
        Publishes a message to a topic, authenticates the publisher first before sending the message
        
        args:/
        kwars: this dictionary represents all the necessary fields in the protobuf message (see .get_required_keys_for_message())

        logs an error level log if the publishing fails
        """
        msg = self._create_proto_message(**kwargs)
        self._publish_message( msg)