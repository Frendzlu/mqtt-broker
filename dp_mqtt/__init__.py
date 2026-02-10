"""
MQTT Broker Package
"""

from .broker import MQTTBroker, run_broker
from .client import Client, MQTTMessage, MQTTError, setup_client_logging
from .protocol import (
    PacketType, ConnectReturnCode, ProtocolError, MalformedPacketError,
    Codec, ConnectFlags, QoSLevel, TopicUtils
)
from .packets import (
    Packet, ConnectPacket, ConnackPacket, PublishPacket, SubscribePacket,
    SubackPacket, UnsubscribePacket, UnsubackPacket,
    PubackPacket, PubrecPacket, PubrelPacket, PubcompPacket, PingrespPacket,
)
from .session import Session, SessionManager, WillMessage, PendingMessage
from .topics import TopicManager, RetainedMessage

# Helper function for topic matching
def topic_matches_filter(topic: str, filter_pattern: str) -> bool:
    """Check if a topic name matches a topic filter pattern."""
    return TopicUtils.topic_matches_filter(topic, filter_pattern)

__version__ = "0.1.0"
__all__ = [
    # Broker
    "MQTTBroker",
    "run_broker",
    # Client
    "Client",
    "MQTTMessage",
    "MQTTError",
    "setup_client_logging",
    # Protocol
    "PacketType",
    "ConnectReturnCode", 
    "ProtocolError",
    "MalformedPacketError",
    "Codec",
    "ConnectFlags",
    "QoSLevel",
    "TopicUtils",
    # Packets
    "Packet",
    "ConnectPacket",
    "ConnackPacket",
    "PublishPacket",
    "SubscribePacket",
    "SubackPacket",
    "UnsubscribePacket",
    "UnsubackPacket",
    "PubackPacket",
    "PubrecPacket",
    "PubrelPacket",
    "PubcompPacket",
    "PingrespPacket",
    # Session
    "Session",
    "SessionManager",
    "WillMessage",
    "PendingMessage",
    # Topics
    "TopicManager",
    "RetainedMessage",
    "topic_matches_filter",
]
