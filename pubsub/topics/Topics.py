from google.cloud import pubsub_v1

class Topic:
     
    _publisher : pubsub_v1.PublisherClient
    _subscriber: pubsub_v1.SubscriberClient
    def __init__(self):
        pass

    def get_all_topics_in_project(self) -> list[str]:
        """
        Returns all Topic objects existing in the project
        """
        if self._topics:
            return [topic.name for topic in self._topics]
        else:
            self._topics = self._subscriber.list_topics(request={"project": f"projects/{self._project_id}"})
            return self.get_all_topics_in_project()
        
    def check_topic_id_available(self, topic_id: str) -> bool:
        """Checks if the topic ID exists in the project"""
        return any([topic.name == topic_id for topic in self._topics])