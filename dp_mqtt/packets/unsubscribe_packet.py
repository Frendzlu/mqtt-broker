"""UNSUBSCRIBE packet for topic unsubscription."""

import struct
from dataclasses import dataclass, field
from typing import List, TYPE_CHECKING

from dp_mqtt.packets.packet import Packet
from dp_mqtt.protocol.codec import Codec
from dp_mqtt.protocol.malformed_packet_error import MalformedPacketError
from dp_mqtt.protocol.protocol_error import ProtocolError

if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker

from dp_mqtt.packets.unsuback_packet import UnsubackPacket


@dataclass
class UnsubscribePacket(Packet):
    """UNSUBSCRIBE packet for topic unsubscription."""
    packet_id: int
    topics: List[str] = field(default_factory=list)
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle UNSUBSCRIBE packet."""
        import logging
        logger = logging.getLogger(__name__)
        
        assert client.session is not None
        for topic_filter in self.topics:
            client.session.remove_subscription(topic_filter)
            logger.debug(f"Client {client.client_id} unsubscribed from {topic_filter}")
        
        # Send UNSUBACK
        await broker._send_packet(client, UnsubackPacket(self.packet_id).to_bytes())

    @classmethod
    def from_bytes(cls, data: bytes) -> "UnsubscribePacket":
        """Parse UNSUBSCRIBE packet payload (after fixed header)."""
        if len(data) < 2:
            raise MalformedPacketError("Missing packet identifier in UNSUBSCRIBE")
        
        packet_id = struct.unpack("!H", data[0:2])[0]
        if packet_id == 0:
            raise ProtocolError("Packet identifier cannot be 0")
        
        offset = 2
        topics = []
        
        while offset < len(data):
            topic_filter, consumed = Codec.decode_string(data, offset)
            offset += consumed
            topics.append(topic_filter)
        
        if not topics:
            raise ProtocolError("UNSUBSCRIBE must have at least one topic filter")
        
        return cls(packet_id=packet_id, topics=topics)
