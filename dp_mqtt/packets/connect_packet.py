from codecs import Codec
from dataclasses import dataclass
import struct
from typing import Optional

from ..protocol.connect_flags import ConnectFlags
from ..protocol.malformed_packet_error import MalformedPacketError
from ..protocol.protocol_error import ProtocolError


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