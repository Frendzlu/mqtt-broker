"""DISCONNECT packet for graceful client disconnection."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dp_mqtt.packets.packet import Packet

if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker


@dataclass
class DisconnectPacket(Packet):
    """DISCONNECT packet for graceful client disconnection."""
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle DISCONNECT packet."""
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
