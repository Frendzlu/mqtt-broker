"""PINGRESP packet for keep-alive response."""

from dataclasses import dataclass

from dp_mqtt.protocol.packet_type import PacketType


@dataclass
class PingrespPacket:
    """PINGRESP packet for keep-alive response."""

    def to_bytes(self) -> bytes:
        """Build PINGRESP packet."""
        return bytes([PacketType.PINGRESP << 4, 0x00])
