"""UNSUBACK packet for unsubscription acknowledgment."""

import struct
from dataclasses import dataclass

from dp_mqtt.protocol.codec import Codec
from dp_mqtt.protocol.packet_type import PacketType


@dataclass
class UnsubackPacket:
    """UNSUBACK packet for unsubscription acknowledgment."""
    packet_id: int

    def to_bytes(self) -> bytes:
        """Build UNSUBACK packet."""
        payload = struct.pack("!H", self.packet_id)
        return bytes([PacketType.UNSUBACK << 4]) + Codec.encode_remaining_length(len(payload)) + payload
