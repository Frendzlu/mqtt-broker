"""
MQTT Client Session Management
Handles session state, subscriptions, and message queuing.
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any
from enum import IntEnum


class QoSLevel(IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


@dataclass
class WillMessage:
    """Client's Will message to be published on unexpected disconnect."""
    topic: str
    payload: bytes
    qos: int
    retain: bool


@dataclass
class PendingMessage:
    """Message pending acknowledgment (for QoS 1 and 2)."""
    packet_id: int
    topic: str
    payload: bytes
    qos: int
    retain: bool
    timestamp: float
    retry_count: int = 0
    state: str = "pending"  # pending, pubrec_received (for QoS 2)


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


class SessionManager:
    """Manages all client sessions."""
    
    def __init__(self):
        self.sessions: Dict[str, Session] = {}
    
    def get_or_create_session(self, client_id: str, clean_session: bool) -> Tuple[Session, bool]:
        """
        Get existing session or create new one.
        Returns (session, session_present).
        If clean_session is True, any existing session is cleared.
        """
        session_present = False
        
        if client_id in self.sessions:
            session = self.sessions[client_id]
            if clean_session:
                session.clear()
            else:
                session_present = True
        else:
            session = Session(client_id=client_id)
            self.sessions[client_id] = session
        
        return session, session_present
    
    def remove_session(self, client_id: str) -> None:
        """Remove a session completely."""
        if client_id in self.sessions:
            del self.sessions[client_id]
    
    def has_session(self, client_id: str) -> bool:
        """Check if a session exists."""
        return client_id in self.sessions
