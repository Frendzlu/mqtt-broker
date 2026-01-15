from dataclasses import dataclass
import struct
from typing import Optional
from typing import TYPE_CHECKING

from dp_mqtt.packets.puback_packet import PubackPacket
from dp_mqtt.packets.pubrec_packet import PubrecPacket

if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker

from dp_mqtt.packets.packet import Packet
from dp_mqtt.protocol.malformed_packet_error import MalformedPacketError
from dp_mqtt.protocol.packet_type import PacketType
from dp_mqtt.protocol.protocol_error import ProtocolError
from dp_mqtt.protocol.codec import Codec


@dataclass
class PublishPacket(Packet):
    """PUBLISH packet for message delivery."""
    topic: str
    payload: bytes = b""
    qos: int = 0
    retain: bool = False
    dup: bool = False
    packet_id: Optional[int] = None  # None for QoS 0
    flags_byte: int = 0  # Store flags for parsing
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBLISH packet."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.debug(f"PUBLISH from {client.client_id}: topic={self.topic}, qos={self.qos}, retain={self.retain}")
        
        # Handle retained message
        if self.retain:
            broker.topic_manager.set_retained_message(
                self.topic, self.payload, self.qos
            )
        
        # Send acknowledgment based on QoS
        if self.qos == 1:
            # packet_id is always present for QoS > 0
            assert self.packet_id is not None
            await broker._send_packet(client, PubackPacket(self.packet_id).to_bytes())
        elif self.qos == 2:
            # packet_id is always present for QoS > 0
            assert self.packet_id is not None
            assert client.session is not None
            # Store for duplicate detection
            client.session.pending_incoming_qos2.add(self.packet_id)
            await broker._send_packet(client, PubrecPacket(self.packet_id).to_bytes())
            return  # Don't forward yet, wait for PUBREL
        
        # Forward to subscribers (QoS 0 and 1, or after PUBREL for QoS 2)
        await broker._forward_publish(self.topic, self.payload, self.qos, self.retain)

    @classmethod
    def from_bytes(cls, flags: int, data: bytes) -> "PublishPacket":
        """Parse PUBLISH packet payload (after fixed header)."""
        dup = bool(flags & 0x08)
        qos = (flags >> 1) & 0x03
        retain = bool(flags & 0x01)
        
        # QoS 3 is invalid
        if qos == 3:
            raise ProtocolError("QoS 3 is invalid")
        
        # DUP must be 0 for QoS 0
        if qos == 0 and dup:
            raise ProtocolError("DUP flag must be 0 for QoS 0")
        
        offset = 0
        
        # Topic Name
        topic, consumed = Codec.decode_string(data, offset)
        offset += consumed
        
        # Validate topic (no wildcards allowed in PUBLISH)
        if '#' in topic or '+' in topic:
            raise ProtocolError("Wildcards not allowed in PUBLISH topic")
        
        if not topic:
            raise ProtocolError("Empty topic name not allowed")
        
        # Packet Identifier (only for QoS > 0)
        packet_id = None
        if qos > 0:
            if offset + 2 > len(data):
                raise MalformedPacketError("Missing packet identifier")
            packet_id = struct.unpack("!H", data[offset:offset + 2])[0]
            offset += 2
            if packet_id == 0:
                raise ProtocolError("Packet identifier cannot be 0")
        
        # Payload (rest of data)
        payload = data[offset:]
        
        return cls(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            dup=dup,
            packet_id=packet_id,
            flags_byte=flags,
        )

    def to_bytes(self) -> bytes:
        """Build PUBLISH packet."""
        flags = (self.dup << 3) | (self.qos << 1) | self.retain
        
        variable_header = Codec.encode_string(self.topic)
        if self.qos > 0:
            if self.packet_id is None:
                raise ValueError("Packet ID required for QoS > 0")
            variable_header += struct.pack("!H", self.packet_id)
        
        packet_payload = variable_header + self.payload
        return bytes([(PacketType.PUBLISH << 4) | flags]) + Codec.encode_remaining_length(len(packet_payload)) + packet_payload