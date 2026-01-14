from abc import ABC, abstractmethod
from typing import Optional
from .protocol import GenericPacket, PacketType
from .broker import ClientConnection

class MQTTPacketFactory(ABC):
    """Abstract factory for creating MQTT packet instances."""

    @abstractmethod
    def construct_packet(self, packet_type: PacketType, flags: int, payload: bytes) -> Optional[GenericPacket]:
        pass