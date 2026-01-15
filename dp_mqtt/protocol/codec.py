# Expected flags for each packet type (None means variable flags like PUBLISH)
import struct
from typing import Tuple
from .malformed_packet_error import MalformedPacketError
from .protocol_error import ProtocolError
from .packet_type import PacketType


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