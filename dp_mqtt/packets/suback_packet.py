"""SUBACK packet for subscription acknowledgment."""

import struct
from dataclasses import dataclass, field
from typing import List

from dp_mqtt.protocol.codec import Codec
from dp_mqtt.protocol.packet_type import PacketType


@dataclass
class SubackPacket:
    """SUBACK packet for subscription acknowledgment."""
    packet_id: int
    return_codes: List[int] = field(default_factory=list)  # 0x00, 0x01, 0x02 (granted QoS) or 0x80 (failure)

    def to_bytes(self) -> bytes:
        """Build SUBACK packet."""
        # Valid return codes: 0x00, 0x01, 0x02 (granted QoS), 0x80 (failure)
        for code in self.return_codes:
            if code not in (0x00, 0x01, 0x02, 0x80):
                raise ValueError(f"Invalid SUBACK return code: {code}")
        
        payload = struct.pack("!H", self.packet_id) + bytes(self.return_codes)
        return bytes([PacketType.SUBACK << 4]) + Codec.encode_remaining_length(len(payload)) + payload
