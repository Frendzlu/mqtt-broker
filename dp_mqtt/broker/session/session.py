from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from .pending_message import PendingMessage

@dataclass
class Session:
    """
    Client session state.
    Persisted across connections when clean_session=False.
    """
    client_id: str
    subscriptions: Dict[str, int] = field(default_factory=dict)  # topic_filter -> qos
    pending_outgoing: Dict[int, PendingMessage] = field(default_factory=dict)  # packet_id -> message
    pending_incoming_qos2: Set[int] = field(default_factory=set)  # packet_ids for QoS 2 received but not complete
    next_packet_id: int = 1
    
    def get_next_packet_id(self) -> int:
        """
        Get next available packet ID.
        Must be non-zero and not currently in use.
        """
        start = self.next_packet_id
        while True:
            packet_id = self.next_packet_id
            self.next_packet_id = (self.next_packet_id % 65535) + 1
            
            # Check if ID is not in use
            if packet_id not in self.pending_outgoing and packet_id not in self.pending_incoming_qos2:
                return packet_id
            
            # Avoid infinite loop
            if self.next_packet_id == start:
                raise RuntimeError("No available packet IDs")
    
    def add_subscription(self, topic_filter: str, qos: int) -> None:
        """Add or update subscription."""
        self.subscriptions[topic_filter] = qos
    
    def remove_subscription(self, topic_filter: str) -> bool:
        """Remove subscription. Returns True if it existed."""
        if topic_filter in self.subscriptions:
            del self.subscriptions[topic_filter]
            return True
        return False
    
    def get_matching_qos(self, topic: str, topic_matches_filter) -> Optional[int]:
        """
        Get the highest QoS among matching subscriptions for a topic.
        Returns None if no subscription matches.
        """
        matching_qos = None
        for filter_pattern, qos in self.subscriptions.items():
            if topic_matches_filter(topic, filter_pattern):
                if matching_qos is None or qos > matching_qos:
                    matching_qos = qos
        return matching_qos
    
    def clear(self) -> None:
        """Clear all session state."""
        self.subscriptions.clear()
        self.pending_outgoing.clear()
        self.pending_incoming_qos2.clear()
        self.next_packet_id = 1