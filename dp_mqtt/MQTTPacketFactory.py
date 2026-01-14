from abc import ABC, abstractmethod
from protocol import PacketType
from broker import ClientConnection

class MQTTPacketFactory(ABC):
    @abstractmethod
    def construct_packet(self, client: ClientConnection, packet_type: PacketType, flags: int, payload: bytes):
        pass