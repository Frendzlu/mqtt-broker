from dataclasses import dataclass
import struct
from typing import TYPE_CHECKING

from dp_mqtt.protocol.malformed_packet_error import MalformedPacketError
from dp_mqtt.protocol.packet_type import PacketType
from dp_mqtt.protocol.codec import Codec

if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker
    
from dp_mqtt.packets.packet import Packet


@dataclass
class PubackPacket(Packet):
    """PUBACK packet for QoS 1 acknowledgment."""
    packet_id: int
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBACK packet."""
        import logging
        logger = logging.getLogger(__name__)
        
        assert client.session is not None
        if self.packet_id in client.session.pending_outgoing:
            del client.session.pending_outgoing[self.packet_id]
            logger.debug(f"PUBACK received for packet {self.packet_id} from {client.client_id}")
        else:
            logger.warning(f"Unexpected PUBACK for packet {self.packet_id} from {client.client_id}")

    @classmethod
    def from_bytes(cls, data: bytes) -> "PubackPacket":
        """Parse PUBACK packet payload."""
        if len(data) < 2:
            raise MalformedPacketError("Missing packet identifier")
        packet_id = struct.unpack("!H", data[0:2])[0]
        return cls(packet_id=packet_id)

    def to_bytes(self) -> bytes:
        """Build PUBACK packet."""
        payload = struct.pack("!H", self.packet_id)
        return bytes([PacketType.PUBACK << 4]) + Codec.encode_remaining_length(len(payload)) + payload