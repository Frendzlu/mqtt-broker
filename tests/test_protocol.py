"""
Tests for MQTT Protocol parsing and encoding.
"""

import pytest
from dp_mqtt.protocol import (
    Codec, PacketType, ConnectReturnCode, ProtocolError, MalformedPacketError,
    ConnectPacket, ConnackPacket, PublishPacket, SubscribePacket, UnsubscribePacket,
    PubackPacket, PubrecPacket, PubrelPacket, PubcompPacket,
    SubackPacket, UnsubackPacket, PingrespPacket,
)


class TestRemainingLength:
    """Tests for variable length encoding/decoding."""
    
    def test_encode_single_byte(self):
        assert Codec.encode_remaining_length(0) == b'\x00'
        assert Codec.encode_remaining_length(127) == b'\x7f'
    
    def test_encode_two_bytes(self):
        assert Codec.encode_remaining_length(128) == b'\x80\x01'
        assert Codec.encode_remaining_length(16383) == b'\xff\x7f'
    
    def test_encode_three_bytes(self):
        assert Codec.encode_remaining_length(16384) == b'\x80\x80\x01'
    
    def test_encode_four_bytes(self):
        assert Codec.encode_remaining_length(2097152) == b'\x80\x80\x80\x01'
    
    def test_encode_max_value(self):
        assert Codec.encode_remaining_length(268435455) == b'\xff\xff\xff\x7f'
    
    def test_decode_single_byte(self):
        assert Codec.decode_remaining_length(b'\x00', 0) == (0, 1)
        assert Codec.decode_remaining_length(b'\x7f', 0) == (127, 1)
    
    def test_decode_two_bytes(self):
        assert Codec.decode_remaining_length(b'\x80\x01', 0) == (128, 2)
        assert Codec.decode_remaining_length(b'\xff\x7f', 0) == (16383, 2)
    
    def test_decode_with_offset(self):
        data = b'\x00\x80\x01'
        assert Codec.decode_remaining_length(data, 1) == (128, 2)
    
    def test_roundtrip(self):
        for value in [0, 1, 127, 128, 16383, 16384, 2097151, 268435455]:
            encoded = Codec.encode_remaining_length(value)
            decoded, _ = Codec.decode_remaining_length(encoded, 0)
            assert decoded == value


class TestStringEncoding:
    """Tests for MQTT string encoding/decoding."""
    
    def test_encode_empty_string(self):
        assert Codec.encode_string("") == b'\x00\x00'
    
    def test_encode_simple_string(self):
        assert Codec.encode_string("hello") == b'\x00\x05hello'
    
    def test_encode_unicode_string(self):
        result = Codec.encode_string("héllo")
        assert result[0:2] == b'\x00\x06'  # 6 bytes for UTF-8
    
    def test_decode_empty_string(self):
        s, consumed = Codec.decode_string(b'\x00\x00', 0)
        assert s == ""
        assert consumed == 2
    
    def test_decode_simple_string(self):
        s, consumed = Codec.decode_string(b'\x00\x05hello', 0)
        assert s == "hello"
        assert consumed == 7
    
    def test_decode_with_null_raises(self):
        # String containing null character
        with pytest.raises(MalformedPacketError):
            Codec.decode_string(b'\x00\x03a\x00b', 0)
    
    def test_roundtrip(self):
        for s in ["", "test", "hello/world", "topic/with/levels"]:
            encoded = Codec.encode_string(s)
            decoded, _ = Codec.decode_string(encoded, 0)
            assert decoded == s


class TestTopicMatching:
    """Tests for topic filter matching."""
    
    def test_exact_match(self):
        assert Codec.topic_matches_filter("sport/tennis", "sport/tennis") is True
        assert Codec.topic_matches_filter("sport/tennis", "sport/football") is False
    
    def test_single_level_wildcard(self):
        assert Codec.topic_matches_filter("sport/tennis/player1", "sport/+/player1") is True
        assert Codec.topic_matches_filter("sport/football/player1", "sport/+/player1") is True
        assert Codec.topic_matches_filter("sport/tennis/player1/stats", "sport/+/player1") is False
    
    def test_multi_level_wildcard(self):
        assert Codec.topic_matches_filter("sport/tennis/player1", "sport/#") is True
        assert Codec.topic_matches_filter("sport/tennis/player1/stats", "sport/#") is True
        assert Codec.topic_matches_filter("sport", "sport/#") is True
        assert Codec.topic_matches_filter("finance/stock", "sport/#") is False
    
    def test_combined_wildcards(self):
        assert Codec.topic_matches_filter("sport/tennis/player1", "sport/+/#") is True
        assert Codec.topic_matches_filter("sport/tennis/player1/stats", "sport/+/#") is True
    
    def test_system_topics(self):
        # Topics starting with $ should not match leading wildcards
        assert Codec.topic_matches_filter("$SYS/broker/clients", "#") is False
        assert Codec.topic_matches_filter("$SYS/broker/clients", "+/broker/clients") is False
        # But explicit matches should work
        assert Codec.topic_matches_filter("$SYS/broker/clients", "$SYS/#") is True


class TestTopicFilterValidation:
    """Tests for topic filter syntax validation."""
    
    def test_valid_filters(self):
        Codec.validate_topic_filter("sport/tennis")
        Codec.validate_topic_filter("sport/+")
        Codec.validate_topic_filter("sport/#")
        Codec.validate_topic_filter("+/tennis")
        Codec.validate_topic_filter("#")
        Codec.validate_topic_filter("+")
    
    def test_invalid_multi_level_wildcard(self):
        with pytest.raises(ProtocolError):
            Codec.validate_topic_filter("sport/#/player")  # # not at end
        with pytest.raises(ProtocolError):
            Codec.validate_topic_filter("sport/te#t")  # # not alone
    
    def test_invalid_single_level_wildcard(self):
        with pytest.raises(ProtocolError):
            Codec.validate_topic_filter("sport/te+t")  # + not alone


class TestPacketBuilding:
    """Tests for building MQTT packets."""
    
    def test_build_connack_accepted(self):
        packet = ConnackPacket(session_present=False, return_code=ConnectReturnCode.ACCEPTED).to_bytes()
        assert packet[0] == (PacketType.CONNACK << 4)
        assert packet[1] == 2  # Remaining length
        assert packet[2] == 0  # Session present = 0
        assert packet[3] == 0  # Return code = 0
    
    def test_build_connack_with_session(self):
        packet = ConnackPacket(session_present=True, return_code=ConnectReturnCode.ACCEPTED).to_bytes()
        assert packet[2] == 1  # Session present = 1
    
    def test_build_publish_qos0(self):
        packet = PublishPacket(topic="test/topic", payload=b"hello", qos=0).to_bytes()
        assert packet[0] == (PacketType.PUBLISH << 4)
    
    def test_build_publish_qos1(self):
        packet = PublishPacket(topic="test/topic", payload=b"hello", qos=1, packet_id=1).to_bytes()
        assert packet[0] == (PacketType.PUBLISH << 4) | 0x02  # QoS 1
    
    def test_build_publish_retain(self):
        packet = PublishPacket(topic="test/topic", payload=b"hello", qos=0, retain=True).to_bytes()
        assert packet[0] & 0x01 == 1  # Retain flag
    
    def test_build_puback(self):
        packet = PubackPacket(packet_id=123).to_bytes()
        assert packet[0] == (PacketType.PUBACK << 4)
        assert packet[2:4] == b'\x00\x7b'  # Packet ID 123
    
    def test_build_pubrel_flags(self):
        packet = PubrelPacket(packet_id=1).to_bytes()
        # PUBREL must have flags 0x02
        assert packet[0] == (PacketType.PUBREL << 4) | 0x02
    
    def test_build_suback(self):
        packet = SubackPacket(packet_id=1, return_codes=[0x00, 0x01, 0x02]).to_bytes()
        assert packet[0] == (PacketType.SUBACK << 4)
        assert packet[-3:] == b'\x00\x01\x02'  # Return codes
    
    def test_build_pingresp(self):
        packet = PingrespPacket().to_bytes()
        assert packet == bytes([PacketType.PINGRESP << 4, 0x00])


class TestPacketFlagsValidation:
    """Tests for packet flags validation."""
    
    def test_reserved_packet_types(self):
        with pytest.raises(ProtocolError):
            Codec.validate_packet_flags(PacketType.RESERVED_0, 0)
        with pytest.raises(ProtocolError):
            Codec.validate_packet_flags(PacketType.RESERVED_15, 0)
    
    def test_connect_flags(self):
        Codec.validate_packet_flags(PacketType.CONNECT, 0)
        with pytest.raises(ProtocolError):
            Codec.validate_packet_flags(PacketType.CONNECT, 1)
    
    def test_subscribe_flags(self):
        Codec.validate_packet_flags(PacketType.SUBSCRIBE, 0x02)
        with pytest.raises(ProtocolError):
            Codec.validate_packet_flags(PacketType.SUBSCRIBE, 0x00)
    
    def test_pubrel_flags(self):
        Codec.validate_packet_flags(PacketType.PUBREL, 0x02)
        with pytest.raises(ProtocolError):
            Codec.validate_packet_flags(PacketType.PUBREL, 0x00)
