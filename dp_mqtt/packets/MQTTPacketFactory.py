from abc import ABC, abstractmethod
from typing import Optional
from dp_mqtt.protocol import Packet, PacketType

class MQTTPacketFactory(ABC):
    """Abstract factory for creating MQTT packet instances."""

    @abstractmethod
    def construct_packet(self, packet_type: PacketType, flags: int, payload: bytes) -> Optional[Packet]:
        pass