class Publisher_boilerplate:
    def __init__(self, project_id: str) -> None:
        '''Initializes a Publisher class with the project_id'''
        pass
    def try_authenticate_google_cloud(self, service_account_path: str) -> bool:
        """Tries to authenticate the service account with the service key in JSON format"""
        pass
    
    def get_topic_id(self, topic_name: str) -> str:
        """Gets the right topic ID asked by the user from the list of topics"""
        pass
    

    def get_topic_path(self, project_id: str, topic_name: str) -> str:
        """Returns the full path of the topic within the project ID"""
        pass

    def create_proto_message(self, fields) -> bytes:
        """Converts a message with multiple fields to a message of bytes, used for the proto message"""
        pass

    def publish_message(self, topic_path: str, message: bytes) -> None:
        """Publishes the message to the specified topic, using the topic path and message"""
        pass

    def publish_message_to_cloud(self, fields) -> None:
        """Publishes a message to a topic, authenticates itself first before sending the message"""
        pass