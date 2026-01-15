from dataclasses import dataclass
import struct

from .packet import Packet

from typing import TYPE_CHECKING

from ..protocol.malformed_packet_error import MalformedPacketError
from ..protocol.packet_type import PacketType
from ..protocol.codec import Codec
if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker

@dataclass
class PubcompPacket(Packet):
    """PUBCOMP packet for QoS 2 (step 3 of 4-way handshake)."""
    packet_id: int
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBCOMP packet."""
        import logging
        logger = logging.getLogger(__name__)
        
        assert client.session is not None
        if self.packet_id in client.session.pending_outgoing:
            del client.session.pending_outgoing[self.packet_id]
            logger.debug(f"PUBCOMP received for packet {self.packet_id} from {client.client_id}")
        else:
            logger.warning(f"Unexpected PUBCOMP for packet {self.packet_id} from {client.client_id}")

    @classmethod
    def from_bytes(cls, data: bytes) -> "PubcompPacket":
        """Parse PUBCOMP packet payload."""
        if len(data) < 2:
            raise MalformedPacketError("Missing packet identifier")
        packet_id = struct.unpack("!H", data[0:2])[0]
        return cls(packet_id=packet_id)

    def to_bytes(self) -> bytes:
        """Build PUBCOMP packet."""
        payload = struct.pack("!H", self.packet_id)
        return bytes([PacketType.PUBCOMP << 4]) + Codec.encode_remaining_length(len(payload)) + payload