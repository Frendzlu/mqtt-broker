from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker

class Packet(ABC):
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