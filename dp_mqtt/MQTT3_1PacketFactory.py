from typing import Optional
from .MQTTPacketFactory import MQTTPacketFactory
from .protocol import (
    PacketType,
    PublishPacket,
    PubackPacket,
    PubrecPacket,
    PubrelPacket,
    PubcompPacket,
    SubscribePacket,
    UnsubscribePacket,
    PingreqPacket,
    DisconnectPacket,
    GenericPacket
)

class MQTT3_1PacketFactory(MQTTPacketFactory):
    def __init__(self):
        super().__init__()

    def construct_packet(self, packet_type: PacketType, flags: int, payload: bytes) -> Optional[GenericPacket]:
        if packet_type == PacketType.PUBLISH:
            packet = PublishPacket.from_bytes(flags, payload)
        elif packet_type == PacketType.PUBACK:
            packet = PubackPacket.from_bytes(payload)
        elif packet_type == PacketType.PUBREC:
            packet = PubrecPacket.from_bytes(payload)
        elif packet_type == PacketType.PUBREL:
            packet = PubrelPacket.from_bytes(payload)
        elif packet_type == PacketType.PUBCOMP:
            packet = PubcompPacket.from_bytes(payload)
        elif packet_type == PacketType.SUBSCRIBE:
            packet = SubscribePacket.from_bytes(payload)
        elif packet_type == PacketType.UNSUBSCRIBE:
            packet = UnsubscribePacket.from_bytes(payload)
        elif packet_type == PacketType.PINGREQ:
            packet = PingreqPacket.from_bytes(payload)
        elif packet_type == PacketType.DISCONNECT:
            packet = DisconnectPacket.from_bytes(payload)
        else:
            return None

        return packet