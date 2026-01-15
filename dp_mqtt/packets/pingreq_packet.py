"""PINGREQ packet for keep-alive ping."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dp_mqtt.packets.packet import Packet

if TYPE_CHECKING:
    from dp_mqtt.broker import ClientConnection, MQTTBroker

from dp_mqtt.packets.pingresp_packet import PingrespPacket


@dataclass
class PingreqPacket(Packet):
    """PINGREQ packet for keep-alive ping."""
    
    async def handle(self, broker: 'MQTTBroker', client: 'ClientConnection') -> None:
        """Handle PINGREQ packet."""
        import logging
        logger = logging.getLogger(__name__)
        
        await broker._send_packet(client, PingrespPacket().to_bytes())
        logger.debug(f"PINGRESP sent to {client.client_id}")
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "PingreqPacket":
        """Parse PINGREQ packet - no payload."""
        return cls()
