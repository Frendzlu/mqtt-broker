from dataclasses import dataclass, field
import struct
from typing import List, Tuple

from dp_mqtt.packets.packet import Packet
from typing import TYPE_CHECKING

from dp_mqtt.protocol import SubackPacket
from dp_mqtt.protocol.malformed_packet_error import MalformedPacketError
from dp_mqtt.protocol.protocol_error import ProtocolError
from dp_mqtt.protocol.codec import Codec
if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker


@dataclass
class SubscribePacket(Packet):
    """SUBSCRIBE packet for topic subscription."""
    packet_id: int
    topics: List[Tuple[str, int]] = field(default_factory=list)  # List of (topic_filter, requested_qos)
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle SUBSCRIBE packet."""
        import logging
        from dp_mqtt.broker.topics import topic_matches_filter
        logger = logging.getLogger(__name__)
        
        return_codes = []
        
        assert client.session is not None
        for topic_filter, requested_qos in self.topics:
            # Grant QoS (limited by broker's max QoS)
            granted_qos = min(requested_qos, broker.max_qos)
            
            # Add subscription
            client.session.add_subscription(topic_filter, granted_qos)
            return_codes.append(granted_qos)
            
            logger.debug(f"Client {client.client_id} subscribed to {topic_filter} with QoS {granted_qos}")
            
            # Send retained messages for new subscription
            retained = broker.topic_manager.get_matching_retained_messages(
                topic_filter, topic_matches_filter
            )
            for msg in retained:
                # Use minimum of message QoS and granted QoS
                effective_qos = min(msg.qos, granted_qos)
                await broker._send_publish(
                    client, msg.topic, msg.payload, effective_qos, retain=True
                )
        
        # Send SUBACK
        await broker._send_packet(client, SubackPacket(self.packet_id, return_codes).to_bytes())

    @classmethod
    def from_bytes(cls, data: bytes) -> "SubscribePacket":
        """Parse SUBSCRIBE packet payload (after fixed header)."""
        if len(data) < 2:
            raise MalformedPacketError("Missing packet identifier in SUBSCRIBE")
        
        packet_id = struct.unpack("!H", data[0:2])[0]
        if packet_id == 0:
            raise ProtocolError("Packet identifier cannot be 0")
        
        offset = 2
        topics = []
        
        while offset < len(data):
            # Topic filter
            topic_filter, consumed = Codec.decode_string(data, offset)
            offset += consumed
            
            # Requested QoS
            if offset >= len(data):
                raise MalformedPacketError("Missing QoS in SUBSCRIBE")
            
            qos = data[offset]
            offset += 1
            
            # Upper 6 bits must be 0, and QoS must not be 3
            if qos & 0xFC or qos == 3:
                raise ProtocolError("Invalid QoS in SUBSCRIBE")
            
            # Validate topic filter
            Codec.validate_topic_filter(topic_filter)
            
            topics.append((topic_filter, qos))
        
        if not topics:
            raise ProtocolError("SUBSCRIBE must have at least one topic filter")
        
        return cls(packet_id=packet_id, topics=topics)