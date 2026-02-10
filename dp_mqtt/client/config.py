"""
MQTT Client Configuration
"""

from dataclasses import dataclass
from typing import Optional, Union


@dataclass
class MQTTClientConfig:
    """Client configuration."""
    client_id: str = ""
    clean_session: bool = True
    keep_alive: int = 60
    username: Optional[str] = None
    password: Optional[str] = None
    will_topic: Optional[str] = None
    will_payload: bytes = b""
    will_qos: int = 0
    will_retain: bool = False
