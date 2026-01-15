from dataclasses import dataclass
import struct
from typing import TYPE_CHECKING

from .pubrel_packet import PubrelPacket
from ..protocol.malformed_packet_error import MalformedPacketError
from ..protocol.packet_type import PacketType
from ..protocol.codec import Codec

if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker
    
from .packet import Packet


@dataclass
class PubrecPacket(Packet):
    """PUBREC packet for QoS 2 (step 1 of 4-way handshake)."""
    packet_id: int
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBREC packet."""
        import logging
        logger = logging.getLogger(__name__)
        
        assert client.session is not None
        if self.packet_id in client.session.pending_outgoing:
            client.session.pending_outgoing[self.packet_id].state = "pubrec_received"
            await broker._send_packet(client, PubrelPacket(self.packet_id).to_bytes())
            logger.debug(f"PUBREC received, PUBREL sent for packet {self.packet_id}")
        else:
            logger.warning(f"Unexpected PUBREC for packet {self.packet_id} from {client.client_id}")

    @classmethod
    def from_bytes(cls, data: bytes) -> "PubrecPacket":
        """Parse PUBREC packet payload."""
        if len(data) < 2:
            raise MalformedPacketError("Missing packet identifier")
        packet_id = struct.unpack("!H", data[0:2])[0]
        return cls(packet_id=packet_id)

    def to_bytes(self) -> bytes:
        """Build PUBREC packet."""
        payload = struct.pack("!H", self.packet_id)
        return bytes([PacketType.PUBREC << 4]) + Codec.encode_remaining_length(len(payload)) + payload