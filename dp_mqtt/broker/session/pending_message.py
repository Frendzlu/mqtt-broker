from dataclasses import dataclass


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