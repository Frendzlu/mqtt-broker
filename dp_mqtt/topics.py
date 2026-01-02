"""
MQTT Topic Management
Handles retained messages and topic matching.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import time


@dataclass
class RetainedMessage:
    """A retained message for a topic."""
    topic: str
    payload: bytes
    qos: int
    timestamp: float


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


def validate_topic_name(topic: str) -> bool:
    """
    Validate a topic name (for PUBLISH).
    Topic names cannot contain wildcards.
    """
    if not topic:
        return False
    if '#' in topic or '+' in topic:
        return False
    # Check for valid UTF-8 is done at string parsing level
    return True


def validate_topic_filter(topic_filter: str) -> bool:
    """
    Validate a topic filter (for SUBSCRIBE).
    + can only occupy an entire level.
    # can only be at the end and must occupy entire level.
    """
    if not topic_filter:
        return False
    
    levels = topic_filter.split('/')
    
    for i, level in enumerate(levels):
        if '#' in level:
            # # must be alone and at the end
            if level != '#' or i != len(levels) - 1:
                return False
        elif '+' in level:
            # + must be alone in its level
            if level != '+':
                return False
    
    return True


def topic_matches_filter(topic: str, filter_pattern: str) -> bool:
    """
    Check if a topic name matches a topic filter pattern.
    Supports + (single level) and # (multi-level) wildcards.
    
    Topics starting with $ are system topics and should not match 
    filters starting with # or + at the first level.
    """
    # System topics special handling
    if topic.startswith('$') and filter_pattern.startswith(('+', '#')):
        return False
    
    topic_levels = topic.split('/')
    filter_levels = filter_pattern.split('/')
    
    topic_idx = 0
    filter_idx = 0
    
    while filter_idx < len(filter_levels):
        filter_level = filter_levels[filter_idx]
        
        if filter_level == '#':
            # Multi-level wildcard matches everything from here
            return True
        
        if topic_idx >= len(topic_levels):
            # Topic is shorter than filter
            return False
        
        topic_level = topic_levels[topic_idx]
        
        if filter_level == '+':
            # Single-level wildcard matches any single level
            pass
        elif filter_level != topic_level:
            # Levels don't match
            return False
        
        topic_idx += 1
        filter_idx += 1
    
    # Both must be exhausted for a match
    return topic_idx == len(topic_levels)
