class Publisher_boilerplate:

    """ Pulisher class to handle sending messages to a specified topic"""

    def __init__(self, project_id: str) -> None:
        """Initializes a Publisher class for pub/sub with the project_id 
        
        args: 
            project_id: takes the project_id as a string in which the topics are specified

        fields:
            self.project_id: stores the project_id as string
            self.publisher: initializes a PublisherClient from google-cloud-pubsub library
            self.topics: initializes a list to store all the topics in the project

        """
        pass
    def authenticate(self, service_account_key: str) -> bool:
        """
        Tries to authenticate the service account with the service key in JSON format, if google cloud is used
        
        returns True if authentication succeeded, otherwise False and an exception
        """
        pass
    
    def get_allTopicsInProject(self) -> list:
        """Creates a new list with all the topics in the project"""
        pass

    def _get_topic_id(self, topic_id: str) -> str:
        """
        Gets the right topic ID asked by the user from the list of topics
        
        Gives an exception if the topic ID wasn't found in the project
        """
        pass
    

    def _get_topic_path(self, topic_id: str) -> str:
        """
        Returns the full path of the topic within the project ID, used to publish the message to the cloud
        
        args:
            topic_id: takes the topic_id as a string

        returns the full topic path within google cloud, used to publish messages
        """
        pass

    def _create_proto_message(self, fields) -> bytes:
        """
        Converts a message with multiple fields to a message of bytes, used for the protobuff message
        
        args: all the fields are dependent on which sensor's data you are sending, these can be anything

        returns the message in the form of bytes, to send to the cloud
        """
        pass

    def _publish_message(self, topic_path: str, message: bytes) -> None:
        """
        Publishes the message to the specified topic, using the topic path and message
        
        args: 
            topic_path: topic path within the cloud environment, gotten from _get_topic_path()
            message: message converted to bytes

        gives an exception if publishing to the cloud failed
        """
        pass

    def publish_message_to_cloud(self, topic_id: str, fields) -> None:
        """
        Publishes a message to a topic, authenticates the publisher first before sending the message
        
        args:
            topic_id: string with the topic_id you want to send messages to
            field: depending on which sensor's, these fields can change

        Gives an exception if the message doesn't send
        """
        pass