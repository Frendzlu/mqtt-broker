from dataclasses import dataclass


@dataclass
class RetainedMessage:
    """A retained message for a topic."""
    topic: str
    payload: bytes
    qos: int
    timestamp: float