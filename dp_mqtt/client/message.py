"""
MQTT Message Representation
"""

import time
from datetime import datetime


class MQTTMessage:
    """Represents a received MQTT message."""
    
    def __init__(self, topic: str, payload: bytes, qos: int, retain: bool):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
        self.mid: int = 0
        self.timestamp: float = time.time()
    
    @property
    def payload_str(self) -> str:
        """Return payload as UTF-8 string."""
        return self.payload.decode('utf-8', errors='replace')
    
    @property
    def timestamp_str(self) -> str:
        """Return timestamp as formatted string."""
        return datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    def __repr__(self) -> str:
        return f"MQTTMessage(topic={self.topic!r}, payload={self.payload!r}, qos={self.qos}, timestamp={self.timestamp})"
    
    def __str__(self) -> str:
        return f"[{self.timestamp_str}] Topic: {self.topic}, QoS: {self.qos}, Retain: {self.retain}, Payload: {self.payload_str}"
