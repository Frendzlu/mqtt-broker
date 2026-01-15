from dataclasses import dataclass


@dataclass
class WillMessage:
    """Client's Will message to be published on unexpected disconnect."""
    topic: str
    payload: bytes
    qos: int
    retain: bool