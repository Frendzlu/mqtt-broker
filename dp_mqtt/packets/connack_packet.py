from dataclasses import dataclass

from dp_mqtt.protocol.connect_return_code import ConnectReturnCode
from dp_mqtt.protocol.packet_type import PacketType
from dp_mqtt.protocol.codec import Codec


@dataclass
class ConnackPacket:
    """CONNACK packet for connection acknowledgment."""
    session_present: bool = False
    return_code: ConnectReturnCode = ConnectReturnCode.ACCEPTED

    def to_bytes(self) -> bytes:
        """Build CONNACK packet."""
        flags = 0x01 if self.session_present else 0x00
        payload = bytes([flags, self.return_code])
        return bytes([PacketType.CONNACK << 4]) + Codec.encode_remaining_length(len(payload)) + payload