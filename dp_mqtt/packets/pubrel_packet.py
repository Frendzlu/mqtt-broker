from dataclasses import dataclass
import struct
from typing import TYPE_CHECKING

from dp_mqtt.packets.pubcomp_packet import PubcompPacket
from dp_mqtt.protocol.malformed_packet_error import MalformedPacketError
from dp_mqtt.protocol.packet_type import PacketType
from dp_mqtt.protocol.codec import Codec

if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker

from dp_mqtt.packets.packet import Packet


@dataclass
class PubrelPacket(Packet):
    """PUBREL packet for QoS 2 (step 2 of 4-way handshake)."""
    packet_id: int
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBREL packet."""
        import logging
        logger = logging.getLogger(__name__)
        
        assert client.session is not None
        if self.packet_id in client.session.pending_incoming_qos2:
            client.session.pending_incoming_qos2.discard(self.packet_id)
            await broker._send_packet(client, PubcompPacket(self.packet_id).to_bytes())
            logger.debug(f"PUBREL received, PUBCOMP sent for packet {self.packet_id}")
        else:
            # Still send PUBCOMP per spec
            await broker._send_packet(client, PubcompPacket(self.packet_id).to_bytes())
            logger.warning(f"PUBREL for unknown packet {self.packet_id} from {client.client_id}")

    @classmethod
    def from_bytes(cls, data: bytes) -> "PubrelPacket":
        """Parse PUBREL packet payload."""
        if len(data) < 2:
            raise MalformedPacketError("Missing packet identifier")
        packet_id = struct.unpack("!H", data[0:2])[0]
        return cls(packet_id=packet_id)

    def to_bytes(self) -> bytes:
        """Build PUBREL packet (flags must be 0x02)."""
        payload = struct.pack("!H", self.packet_id)
        return bytes([(PacketType.PUBREL << 4) | 0x02]) + Codec.encode_remaining_length(len(payload)) + payload