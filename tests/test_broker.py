"""
Integration tests for MQTT Broker.
"""

import pytest
import asyncio
import struct
from mqtt_broker.broker import MQTTBroker, ClientConnection
from mqtt_broker.protocol import (
    encode_string, encode_remaining_length, PacketType
)


def build_connect_packet(
    client_id: str = "test_client",
    clean_session: bool = True,
    keep_alive: int = 60,
    will_flag: bool = False,
    will_topic: str = "",
    will_message: bytes = b"",
    will_qos: int = 0,
    will_retain: bool = False,
    username: str | None = None,
    password: bytes | None = None
) -> bytes:
    """Build a CONNECT packet for testing."""
    # Variable header
    variable_header = encode_string("MQTT")  # Protocol name
    variable_header += bytes([4])  # Protocol level
    
    # Connect flags
    flags = 0
    if clean_session:
        flags |= 0x02
    if will_flag:
        flags |= 0x04
        flags |= (will_qos & 0x03) << 3
        if will_retain:
            flags |= 0x20
    if password is not None:
        flags |= 0x40
    if username is not None:
        flags |= 0x80
    
    variable_header += bytes([flags])
    variable_header += struct.pack("!H", keep_alive)
    
    # Payload
    payload = encode_string(client_id)
    
    if will_flag:
        payload += encode_string(will_topic)
        payload += struct.pack("!H", len(will_message)) + will_message
    
    if username is not None:
        payload += encode_string(username)
    
    if password is not None:
        payload += struct.pack("!H", len(password)) + password
    
    # Fixed header
    remaining = variable_header + payload
    fixed_header = bytes([PacketType.CONNECT << 4])
    fixed_header += encode_remaining_length(len(remaining))
    
    return fixed_header + remaining


def build_subscribe_packet(packet_id: int, topics: list) -> bytes:
    """Build a SUBSCRIBE packet for testing."""
    payload = struct.pack("!H", packet_id)
    
    for topic, qos in topics:
        payload += encode_string(topic)
        payload += bytes([qos])
    
    fixed_header = bytes([(PacketType.SUBSCRIBE << 4) | 0x02])
    fixed_header += encode_remaining_length(len(payload))
    
    return fixed_header + payload


def build_publish_packet(
    topic: str,
    payload: bytes,
    qos: int = 0,
    retain: bool = False,
    dup: bool = False,
    packet_id: int | None = None
) -> bytes:
    """Build a PUBLISH packet for testing."""
    flags = (dup << 3) | (qos << 1) | retain
    
    variable_header = encode_string(topic)
    if qos > 0 and packet_id is not None:
        variable_header += struct.pack("!H", packet_id)
    
    message = variable_header + payload
    
    fixed_header = bytes([(PacketType.PUBLISH << 4) | flags])
    fixed_header += encode_remaining_length(len(message))
    
    return fixed_header + message


def build_pingreq_packet() -> bytes:
    """Build a PINGREQ packet."""
    return bytes([PacketType.PINGREQ << 4, 0x00])


def build_disconnect_packet() -> bytes:
    """Build a DISCONNECT packet."""
    return bytes([PacketType.DISCONNECT << 4, 0x00])


class TestBrokerConnection:
    """Tests for client connection handling."""
    
    @pytest.fixture
    def broker(self):
        return MQTTBroker(host="127.0.0.1", port=0)  # Port 0 = random available
    
    @pytest.mark.asyncio
    async def test_broker_starts_and_stops(self, broker):
        """Test that broker can start and stop cleanly."""
        start_task = asyncio.create_task(broker.start())
        await asyncio.sleep(0.1)  # Let it start
        
        assert broker.running is True
        assert broker.server is not None
        
        await broker.stop()
        start_task.cancel()
        
        try:
            await start_task
        except asyncio.CancelledError:
            pass


class TestProtocolIntegration:
    """Integration tests for protocol handling."""
    
    def test_connect_packet_building(self):
        """Test that we can build valid CONNECT packets."""
        packet = build_connect_packet(client_id="test123")
        
        # Verify packet type
        assert packet[0] >> 4 == PacketType.CONNECT
        
        # Verify it can be parsed
        from mqtt_broker.protocol import parse_fixed_header, parse_connect
        
        ptype, flags, remaining_len, header_size = parse_fixed_header(packet)
        assert ptype == PacketType.CONNECT
        assert flags == 0
        
        payload = packet[header_size:header_size + remaining_len]
        connect = parse_connect(payload)
        
        assert connect.client_id == "test123"
        assert connect.protocol_name == "MQTT"
        assert connect.protocol_level == 4
    
    def test_connect_with_will(self):
        """Test CONNECT packet with will message."""
        packet = build_connect_packet(
            client_id="test",
            will_flag=True,
            will_topic="client/status",
            will_message=b"offline",
            will_qos=1,
            will_retain=True
        )
        
        from mqtt_broker.protocol import parse_fixed_header, parse_connect
        
        ptype, flags, remaining_len, header_size = parse_fixed_header(packet)
        payload = packet[header_size:header_size + remaining_len]
        connect = parse_connect(payload)
        
        assert connect.flags.will_flag is True
        assert connect.will_topic == "client/status"
        assert connect.will_message == b"offline"
        assert connect.flags.will_qos == 1
        assert connect.flags.will_retain is True
    
    def test_subscribe_packet_parsing(self):
        """Test SUBSCRIBE packet parsing."""
        packet = build_subscribe_packet(1, [("sport/tennis", 1), ("sport/#", 2)])
        
        from mqtt_broker.protocol import parse_fixed_header, parse_subscribe
        
        ptype, flags, remaining_len, header_size = parse_fixed_header(packet)
        payload = packet[header_size:header_size + remaining_len]
        subscribe = parse_subscribe(payload)
        
        assert subscribe.packet_id == 1
        assert len(subscribe.topics) == 2
        assert subscribe.topics[0] == ("sport/tennis", 1)
        assert subscribe.topics[1] == ("sport/#", 2)
    
    def test_publish_packet_qos0(self):
        """Test PUBLISH packet with QoS 0."""
        packet = build_publish_packet("test/topic", b"hello", qos=0)
        
        from mqtt_broker.protocol import parse_fixed_header, parse_publish
        
        ptype, flags, remaining_len, header_size = parse_fixed_header(packet)
        payload = packet[header_size:header_size + remaining_len]
        publish = parse_publish(flags, payload)
        
        assert publish.topic == "test/topic"
        assert publish.payload == b"hello"
        assert publish.qos == 0
        assert publish.packet_id is None
    
    def test_publish_packet_qos1(self):
        """Test PUBLISH packet with QoS 1."""
        packet = build_publish_packet("test/topic", b"hello", qos=1, packet_id=123)
        
        from mqtt_broker.protocol import parse_fixed_header, parse_publish
        
        ptype, flags, remaining_len, header_size = parse_fixed_header(packet)
        payload = packet[header_size:header_size + remaining_len]
        publish = parse_publish(flags, payload)
        
        assert publish.qos == 1
        assert publish.packet_id == 123
