import time
from typing import Dict, List, Optional
from .retained_message import RetainedMessage


class TopicManager:
    """Manages retained messages and topic operations."""
    
    def __init__(self):
        self.retained_messages: Dict[str, RetainedMessage] = {}  # topic -> message
    
    def set_retained_message(self, topic: str, payload: bytes, qos: int) -> None:
        """
        Set or update retained message for a topic.
        Empty payload with retain=True clears the retained message.
        """
        if len(payload) == 0:
            # Empty payload clears retained message
            if topic in self.retained_messages:
                del self.retained_messages[topic]
        else:
            self.retained_messages[topic] = RetainedMessage(
                topic=topic,
                payload=payload,
                qos=qos,
                timestamp=time.time()
            )
    
    def get_retained_message(self, topic: str) -> Optional[RetainedMessage]:
        """Get retained message for exact topic."""
        return self.retained_messages.get(topic)
    
    def get_matching_retained_messages(self, topic_filter: str, topic_matches_filter) -> List[RetainedMessage]:
        """
        Get all retained messages matching a topic filter.
        Used when client subscribes to send retained messages.
        """
        matching = []
        for topic, message in self.retained_messages.items():
            if topic_matches_filter(topic, topic_filter):
                matching.append(message)
        return matching
    
    def clear_retained_message(self, topic: str) -> None:
        """Clear retained message for a topic."""
        if topic in self.retained_messages:
            del self.retained_messages[topic]
