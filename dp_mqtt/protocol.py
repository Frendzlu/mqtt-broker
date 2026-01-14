"""
MQTT 3.1.1 Protocol Implementation
Handles packet parsing, encoding/decoding, and protocol validation.
"""

import struct
from enum import IntEnum
from typing import Optional, Tuple, List, Any, TYPE_CHECKING
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from .broker import ClientConnection, MQTTBroker

class PacketType(IntEnum):
    """MQTT Control Packet Types"""
    RESERVED_0 = 0
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14
    RESERVED_15 = 15


class ConnectReturnCode(IntEnum):
    """CONNACK Return Codes"""
    ACCEPTED = 0x00
    UNACCEPTABLE_PROTOCOL = 0x01
    IDENTIFIER_REJECTED = 0x02
    SERVER_UNAVAILABLE = 0x03
    BAD_USERNAME_PASSWORD = 0x04
    NOT_AUTHORIZED = 0x05


class ProtocolError(Exception):
    """Raised when protocol violation is detected - connection should be closed"""
    pass


class MalformedPacketError(ProtocolError):
    """Raised when packet structure is invalid"""
    pass


# Expected flags for each packet type (None means variable flags like PUBLISH)
EXPECTED_FLAGS = {
    PacketType.CONNECT: 0x00,
    PacketType.CONNACK: 0x00,
    PacketType.PUBLISH: None,  # Variable: DUP, QoS, RETAIN
    PacketType.PUBACK: 0x00,
    PacketType.PUBREC: 0x00,
    PacketType.PUBREL: 0x02,  # Must be 0x02
    PacketType.PUBCOMP: 0x00,
    PacketType.SUBSCRIBE: 0x02,  # Must be 0x02
    PacketType.SUBACK: 0x00,
    PacketType.UNSUBSCRIBE: 0x02,  # Must be 0x02
    PacketType.UNSUBACK: 0x00,
    PacketType.PINGREQ: 0x00,
    PacketType.PINGRESP: 0x00,
    PacketType.DISCONNECT: 0x00,
}


class Codec:
    """
    MQTT Protocol Codec utility class.
    Provides encoding/decoding methods for MQTT data types and packet structures.
    """
    
    @staticmethod
    def decode_remaining_length(data: bytes, offset: int = 0) -> Tuple[int, int]:
        """
        Decode variable length remaining length field.
        Returns (remaining_length, bytes_consumed).
        Max 4 bytes, max value 268,435,455.
        """
        multiplier = 1
        value = 0
        bytes_consumed = 0
        
        while True:
            if offset + bytes_consumed >= len(data):
                raise MalformedPacketError("Incomplete remaining length field")
            
            byte = data[offset + bytes_consumed]
            bytes_consumed += 1
            value += (byte & 0x7F) * multiplier
            
            if bytes_consumed > 4:
                raise MalformedPacketError("Remaining length exceeds 4 bytes")
            
            multiplier *= 128
            
            if (byte & 0x80) == 0:
                break
        
        return value, bytes_consumed

    @staticmethod
    def encode_remaining_length(length: int) -> bytes:
        """
        Encode remaining length as variable length field.
        Max value 268,435,455 (0x0FFFFFFF).
        """
        if length > 268435455:
            raise ValueError("Remaining length too large to encode")
        
        result = bytearray()
        while True:
            byte = length % 128
            length = length // 128
            if length > 0:
                byte |= 0x80
            result.append(byte)
            if length == 0:
                break
        
        return bytes(result)

    @staticmethod
    def decode_string(data: bytes, offset: int = 0) -> Tuple[str, int]:
        """
        Decode MQTT UTF-8 string.
        Format: 2 bytes length (big-endian) + UTF-8 encoded string.
        Returns (string, bytes_consumed).
        """
        if offset + 2 > len(data):
            raise MalformedPacketError("Incomplete string length")
        
        length = struct.unpack("!H", data[offset:offset + 2])[0]
        
        if offset + 2 + length > len(data):
            raise MalformedPacketError("Incomplete string data")
        
        string_bytes = data[offset + 2:offset + 2 + length]
        
        # Handle BOM (0xEF 0xBB 0xBF) as U+FEFF
        if string_bytes.startswith(b'\xef\xbb\xbf'):
            string_bytes = b'\xef\xbb\xbf' + string_bytes[3:]  # Keep as is, will decode to U+FEFF
        
        try:
            string = string_bytes.decode('utf-8')
        except UnicodeDecodeError:
            raise MalformedPacketError("Invalid UTF-8 in string")
        
        # Check for null character (U+0000) - not allowed in MQTT strings
        if '\x00' in string:
            raise MalformedPacketError("Null character (U+0000) not allowed in MQTT strings")
        
        return string, 2 + length

    @staticmethod
    def encode_string(s: str) -> bytes:
        """
        Encode string as MQTT UTF-8 string.
        Format: 2 bytes length (big-endian) + UTF-8 encoded string.
        """
        encoded = s.encode('utf-8')
        if len(encoded) > 65535:
            raise ValueError("String too long for MQTT encoding")
        return struct.pack("!H", len(encoded)) + encoded

    @staticmethod
    def decode_binary(data: bytes, offset: int = 0) -> Tuple[bytes, int]:
        """
        Decode binary data with 2-byte length prefix.
        Returns (binary_data, bytes_consumed).
        """
        if offset + 2 > len(data):
            raise MalformedPacketError("Incomplete binary length")
        
        length = struct.unpack("!H", data[offset:offset + 2])[0]
        
        if offset + 2 + length > len(data):
            raise MalformedPacketError("Incomplete binary data")
        
        return data[offset + 2:offset + 2 + length], 2 + length

    @staticmethod
    def encode_binary(data: bytes) -> bytes:
        """Encode binary data with 2-byte length prefix."""
        if len(data) > 65535:
            raise ValueError("Binary data too long for MQTT encoding")
        return struct.pack("!H", len(data)) + data

    @staticmethod
    def validate_packet_flags(packet_type: PacketType, flags: int) -> None:
        """
        Validate fixed header flags for packet type.
        Raises ProtocolError if flags are invalid.
        """
        if packet_type in (PacketType.RESERVED_0, PacketType.RESERVED_15):
            raise ProtocolError(f"Reserved packet type: {packet_type}")
        
        expected = EXPECTED_FLAGS.get(packet_type)
        if expected is not None and flags != expected:
            raise ProtocolError(f"Invalid flags for {packet_type.name}: expected {expected:#x}, got {flags:#x}")

    @staticmethod
    def parse_fixed_header(data: bytes) -> Tuple[PacketType, int, int, int]:
        """
        Parse fixed header.
        Returns (packet_type, flags, remaining_length, header_size).
        """
        if len(data) < 2:
            raise MalformedPacketError("Packet too short for fixed header")
        
        first_byte = data[0]
        packet_type = PacketType(first_byte >> 4)
        flags = first_byte & 0x0F
        
        remaining_length, length_bytes = Codec.decode_remaining_length(data, 1)
        header_size = 1 + length_bytes
        
        return packet_type, flags, remaining_length, header_size

    @staticmethod
    def validate_topic_filter(topic_filter: str) -> None:
        """Validate topic filter syntax."""
        if not topic_filter:
            raise ProtocolError("Empty topic filter not allowed")
        
        levels = topic_filter.split('/')
        
        for i, level in enumerate(levels):
            if '#' in level:
                # Multi-level wildcard must be the entire level and last
                if level != '#' or i != len(levels) - 1:
                    raise ProtocolError("Multi-level wildcard (#) must be alone and at the end")
            elif '+' in level:
                # Single-level wildcard must be the entire level
                if level != '+':
                    raise ProtocolError("Single-level wildcard (+) must occupy entire level")

    @staticmethod
    def topic_matches_filter(topic: str, filter_pattern: str) -> bool:
        """
        Check if a topic name matches a topic filter pattern.
        Supports + (single level) and # (multi-level) wildcards.
        
        Topics starting with $ are system topics and should not match 
        filters starting with # or + at the first level.
        """
        # System topics special handling
        if topic.startswith('$') and filter_pattern.startswith(('+', '#')):
            return False
        
        topic_levels = topic.split('/')
        filter_levels = filter_pattern.split('/')
        
        topic_idx = 0
        filter_idx = 0
        
        while filter_idx < len(filter_levels):
            filter_level = filter_levels[filter_idx]
            
            if filter_level == '#':
                # Multi-level wildcard matches everything from here
                return True
            
            if topic_idx >= len(topic_levels):
                # Topic is shorter than filter
                return False
            
            topic_level = topic_levels[topic_idx]
            
            if filter_level == '+':
                # Single-level wildcard matches any single level
                pass
            elif filter_level != topic_level:
                # Levels don't match
                return False
            
            topic_idx += 1
            filter_idx += 1
        
        # Both must be exhausted for a match
        return topic_idx == len(topic_levels)


class GenericPacket(ABC):
    """
    Abstract base class for all MQTT packets.
    Strategy Pattern: Each packet type implements its own handling logic.
    """
    
    @abstractmethod
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """
        Handle this packet in the context of a broker and client connection.
        
        Args:
            broker: The MQTT broker instance
            client: The client connection that sent this packet
        """
        pass


@dataclass
class ConnectFlags:
    """CONNECT packet flags"""
    clean_session: bool = False
    will_flag: bool = False
    will_qos: int = 0
    will_retain: bool = False
    password_flag: bool = False
    username_flag: bool = False


@dataclass
class ConnectPacket:
    """Parsed CONNECT packet"""
    protocol_name: str
    protocol_level: int
    flags: ConnectFlags
    keep_alive: int
    client_id: str
    will_topic: Optional[str] = None
    will_message: Optional[bytes] = None
    username: Optional[str] = None
    password: Optional[bytes] = None

    @classmethod
    def from_bytes(cls, data: bytes) -> "ConnectPacket":
        """Parse CONNECT packet payload (after fixed header)."""
        offset = 0
        
        # Protocol Name
        protocol_name, consumed = Codec.decode_string(data, offset)
        offset += consumed
        
        if protocol_name != "MQTT":
            raise ProtocolError(f"Invalid protocol name: {protocol_name}")
        
        # Protocol Level
        if offset >= len(data):
            raise MalformedPacketError("Missing protocol level")
        protocol_level = data[offset]
        offset += 1
        
        # For MQTT 3.1.1, protocol level should be 4
        if protocol_level != 4:
            raise ProtocolError(f"Unsupported protocol level: {protocol_level}")
        
        # Connect Flags
        if offset >= len(data):
            raise MalformedPacketError("Missing connect flags")
        
        flags_byte = data[offset]
        offset += 1
        
        # Check reserved bit (must be 0)
        if flags_byte & 0x01:
            raise ProtocolError("Reserved bit in CONNECT flags must be 0")
        
        flags = ConnectFlags(
            clean_session=bool(flags_byte & 0x02),
            will_flag=bool(flags_byte & 0x04),
            will_qos=(flags_byte >> 3) & 0x03,
            will_retain=bool(flags_byte & 0x20),
            password_flag=bool(flags_byte & 0x40),
            username_flag=bool(flags_byte & 0x80),
        )
        
        # Validate Will flags
        if not flags.will_flag:
            if flags.will_qos != 0:
                raise ProtocolError("Will QoS must be 0 when Will Flag is 0")
            if flags.will_retain:
                raise ProtocolError("Will Retain must be 0 when Will Flag is 0")
        else:
            if flags.will_qos == 3:
                raise ProtocolError("Will QoS cannot be 3")
        
        # Password flag requires Username flag
        if flags.password_flag and not flags.username_flag:
            raise ProtocolError("Password flag requires Username flag")
        
        # Keep Alive
        if offset + 2 > len(data):
            raise MalformedPacketError("Missing keep alive")
        keep_alive = struct.unpack("!H", data[offset:offset + 2])[0]
        offset += 2
        
        # Payload
        # Client Identifier
        client_id, consumed = Codec.decode_string(data, offset)
        offset += consumed
        
        # Will Topic and Will Message
        will_topic = None
        will_message = None
        if flags.will_flag:
            will_topic, consumed = Codec.decode_string(data, offset)
            offset += consumed
            will_message, consumed = Codec.decode_binary(data, offset)
            offset += consumed
        
        # Username
        username = None
        if flags.username_flag:
            username, consumed = Codec.decode_string(data, offset)
            offset += consumed
        
        # Password
        password = None
        if flags.password_flag:
            password, consumed = Codec.decode_binary(data, offset)
            offset += consumed
        
        return cls(
            protocol_name=protocol_name,
            protocol_level=protocol_level,
            flags=flags,
            keep_alive=keep_alive,
            client_id=client_id,
            will_topic=will_topic,
            will_message=will_message,
            username=username,
            password=password,
        )


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


@dataclass
class PublishPacket(GenericPacket):
    """PUBLISH packet for message delivery."""
    topic: str
    payload: bytes = b""
    qos: int = 0
    retain: bool = False
    dup: bool = False
    packet_id: Optional[int] = None  # None for QoS 0
    flags_byte: int = 0  # Store flags for parsing
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBLISH packet - Strategy Pattern implementation."""
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


@dataclass
class PubackPacket(GenericPacket):
    """PUBACK packet for QoS 1 acknowledgment."""
    packet_id: int
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBACK packet - Strategy Pattern implementation."""
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


@dataclass
class PubrecPacket(GenericPacket):
    """PUBREC packet for QoS 2 (step 1 of 4-way handshake)."""
    packet_id: int
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBREC packet - Strategy Pattern implementation."""
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


@dataclass
class PubrelPacket(GenericPacket):
    """PUBREL packet for QoS 2 (step 2 of 4-way handshake)."""
    packet_id: int
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBREL packet - Strategy Pattern implementation."""
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


@dataclass
class PubcompPacket(GenericPacket):
    """PUBCOMP packet for QoS 2 (step 3 of 4-way handshake)."""
    packet_id: int
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PUBCOMP packet - Strategy Pattern implementation."""
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


@dataclass
class SubscribePacket(GenericPacket):
    """SUBSCRIBE packet for topic subscription."""
    packet_id: int
    topics: List[Tuple[str, int]] = field(default_factory=list)  # List of (topic_filter, requested_qos)
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle SUBSCRIBE packet - Strategy Pattern implementation."""
        import logging
        from .topics import topic_matches_filter
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


@dataclass
class UnsubscribePacket(GenericPacket):
    """UNSUBSCRIBE packet for topic unsubscription."""
    packet_id: int
    topics: List[str] = field(default_factory=list)
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle UNSUBSCRIBE packet - Strategy Pattern implementation."""
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


@dataclass
class UnsubackPacket:
    """UNSUBACK packet for unsubscription acknowledgment."""
    packet_id: int

    def to_bytes(self) -> bytes:
        """Build UNSUBACK packet."""
        payload = struct.pack("!H", self.packet_id)
        return bytes([PacketType.UNSUBACK << 4]) + Codec.encode_remaining_length(len(payload)) + payload


@dataclass
class PingrespPacket:
    """PINGRESP packet for keep-alive response."""

    def to_bytes(self) -> bytes:
        """Build PINGRESP packet."""
        return bytes([PacketType.PINGRESP << 4, 0x00])


@dataclass
class PingreqPacket(GenericPacket):
    """PINGREQ packet for keep-alive ping."""
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PINGREQ packet - Strategy Pattern implementation."""
        import logging
        logger = logging.getLogger(__name__)
        
        await broker._send_packet(client, PingrespPacket().to_bytes())
        logger.debug(f"PINGRESP sent to {client.client_id}")
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "PingreqPacket":
        """Parse PINGREQ packet - no payload."""
        return cls()


@dataclass
class DisconnectPacket(GenericPacket):
    """DISCONNECT packet for graceful client disconnection."""
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle DISCONNECT packet - Strategy Pattern implementation."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Client {client.client_id} sent DISCONNECT")
        # Clear will message (don't publish on clean disconnect)
        client.will_message = None
        client.connected = False
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "DisconnectPacket":
        """Parse DISCONNECT packet - no payload."""
        return cls()
