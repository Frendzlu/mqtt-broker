"""
MQTT Broker Implementation
Main server handling client connections and message routing.
"""

import asyncio
import logging
from pathlib import Path
import time
from typing import Dict, Optional, Set
from dataclasses import dataclass

from .broker_config import BrokerConfig

from .client_connection import ClientConnection
from ..protocol.topic_utils import TopicUtils

from ..protocol.packet_type import PacketType
from ..protocol.connect_return_code import ConnectReturnCode
from ..protocol.protocol_error import ProtocolError
from ..protocol.malformed_packet_error import MalformedPacketError
from ..protocol.codec import Codec
from ..packets.connect_packet import ConnectPacket
from ..packets.publish_packet import PublishPacket
from ..packets.connack_packet import ConnackPacket
from ..packets.pubrel_packet import PubrelPacket

from ..session import Session, SessionManager, PendingMessage, WillMessage
from ..topics import TopicManager
from ..auth.auth_manager import AuthManager

from ..packets.MQTT3_1PacketFactory import MQTT3_1PacketFactory


logger = logging.getLogger(__name__)


class MQTTBroker:
    """
    MQTT 3.1.1 Broker implementation.
    Handles client connections, message routing, and protocol compliance.
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 1883, max_qos: int = 2, config_path: Optional[str] = None):

        if config_path is None:
            config_path = "config.yaml"
        
        self.config_path = Path(config_path)

        broker_config = BrokerConfig.from_config_file(self.config_path)
        self.auth_manager = AuthManager(config_path)
        
        # Use config values if provided, otherwise use parameters
        self.host = broker_config.host if config_path else host
        self.port = broker_config.port if config_path else port
        self.max_qos = broker_config.max_qos if config_path else max_qos
        
        self.session_manager = SessionManager()
        self.topic_manager = TopicManager()
        
        # Active client connections by client_id
        self.clients: Dict[str, ClientConnection] = {}
        
        # Server state
        self.server: Optional[asyncio.Server] = None
        self.running = False
        
        # Connection timeout for CONNECT packet (seconds)
        self.connect_timeout = 30

        # MQTT factory
        self.factory = MQTT3_1PacketFactory()
    
    async def start(self) -> None:
        """Start the MQTT broker."""
        self.running = True
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port
        )
        
        addr = self.server.sockets[0].getsockname()
        logger.info(f"MQTT Broker started on {addr[0]}:{addr[1]}")
        
        async with self.server:
            await self.server.serve_forever()
    
    async def stop(self) -> None:
        """Stop the MQTT broker."""
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        # Close all client connections
        for client in list(self.clients.values()):
            await self._close_connection(client, publish_will=False)
        
        logger.info("MQTT Broker stopped")
    
    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle a new client connection."""
        addr = writer.get_extra_info('peername')
        address_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"
        
        client = ClientConnection(
            reader=reader,
            writer=writer,
            address=address_str,
            last_activity=time.time()
        )
        
        logger.info(f"New connection from {address_str}")
        
        try:
            # Wait for CONNECT packet with timeout
            first_packet = await asyncio.wait_for(
                self._read_packet(client),
                timeout=self.connect_timeout
            )
            
            if first_packet is None:
                logger.warning(f"Connection closed before CONNECT from {address_str}")
                return
            
            packet_type, flags, payload = first_packet
            
            # First packet MUST be CONNECT
            if packet_type != PacketType.CONNECT:
                logger.warning(f"First packet not CONNECT from {address_str}")
                await self._close_connection(client)
                return
            
            # Process CONNECT
            if not await self._handle_connect(client, flags, payload):
                return
            
            # Start keep-alive monitoring
            keepalive_task = None
            if client.keep_alive > 0:
                keepalive_task = asyncio.create_task(
                    self._monitor_keepalive(client)
                )
            
            # Main packet processing loop
            try:
                while self.running and client.connected:
                    packet = await self._read_packet(client)
                    
                    if packet is None:
                        # Connection closed
                        break
                    
                    packet_type, flags, payload = packet
                    client.last_activity = time.time()
                    
                    # Second CONNECT is protocol error
                    if packet_type == PacketType.CONNECT:
                        logger.warning(f"Second CONNECT from {client.client_id}")
                        await self._close_connection(client)
                        return
                    
                    await self._handle_packet(client, packet_type, flags, payload)
            finally:
                if keepalive_task:
                    keepalive_task.cancel()
                    try:
                        await keepalive_task
                    except asyncio.CancelledError:
                        pass
        
        except asyncio.TimeoutError:
            logger.warning(f"CONNECT timeout from {address_str}")
        except ProtocolError as e:
            logger.warning(f"Protocol error from {address_str}: {e}")
        except Exception as e:
            logger.error(f"Error handling client {address_str}: {e}", exc_info=True)
        finally:
            await self._close_connection(client, publish_will=client.connected)
    
    async def _read_packet(self, client: ClientConnection) -> Optional[tuple]:
        """
        Read a complete MQTT packet from client.
        Returns (packet_type, flags, payload) or None on connection close.
        """
        try:
            # Read fixed header first byte
            first_byte = await client.reader.read(1)
            if not first_byte:
                return None
            
            packet_type = PacketType(first_byte[0] >> 4)
            flags = first_byte[0] & 0x0F
            
            # Read remaining length
            remaining_length = 0
            multiplier = 1
            for _ in range(4):
                byte = await client.reader.read(1)
                if not byte:
                    return None
                remaining_length += (byte[0] & 0x7F) * multiplier
                multiplier *= 128
                if (byte[0] & 0x80) == 0:
                    break
            else:
                raise MalformedPacketError("Remaining length exceeds 4 bytes")
            
            # Read payload
            payload = b""
            if remaining_length > 0:
                payload = await client.reader.readexactly(remaining_length)
            
            # Validate flags
            Codec.validate_packet_flags(packet_type, flags)
            
            return packet_type, flags, payload
        
        except asyncio.IncompleteReadError:
            return None
        except ConnectionResetError:
            return None
    
    async def _handle_connect(self, client: ClientConnection, flags: int, payload: bytes) -> bool:
        """
        Handle CONNECT packet.
        Returns True if connection was accepted, False otherwise.
        """
        try:
            connect = ConnectPacket.from_bytes(payload)
        except ProtocolError as e:
            logger.warning(f"Invalid CONNECT from {client.address}: {e}")
            # Protocol error: close without CONNACK
            await self._close_connection(client)
            return False
        
        client_id = connect.client_id
        
        # Authenticate user
        if not self.auth_manager.authenticate(connect.username, connect.password):
            logger.warning(f"Authentication failed for {client.address}")
            await self._send_connack(client, False, ConnectReturnCode.NOT_AUTHORIZED)
            await self._close_connection(client)
            return False
        
        # Empty client ID handling
        if not client_id:
            if not connect.flags.clean_session:
                # Empty client ID with clean_session=0 is not allowed
                await self._send_connack(client, False, ConnectReturnCode.IDENTIFIER_REJECTED)
                await self._close_connection(client)
                return False
            # Generate unique client ID for empty ID with clean_session=1
            client_id = f"auto_{id(client)}_{int(time.time() * 1000)}"
        
        # Check if client ID is already connected
        if client_id in self.clients:
            # Disconnect existing client
            existing = self.clients[client_id]
            logger.info(f"Disconnecting existing client {client_id} for new connection")
            await self._close_connection(existing, publish_will=True)
        
        # Get or create session
        session, session_present = self.session_manager.get_or_create_session(
            client_id, connect.flags.clean_session
        )
        
        # Set up client connection
        client.client_id = client_id
        client.session = session
        client.keep_alive = connect.keep_alive
        client.clean_session = connect.flags.clean_session
        client.connected = True
        
        # Set up will message
        if connect.flags.will_flag:
            # When will_flag is True, will_topic and will_message are guaranteed to be present
            assert connect.will_topic is not None
            assert connect.will_message is not None
            client.will_message = WillMessage(
                topic=connect.will_topic,
                payload=connect.will_message,
                qos=connect.flags.will_qos,
                retain=connect.flags.will_retain
            )
        
        # Register client
        self.clients[client_id] = client
        
        # Send CONNACK
        # Session present is only true if clean_session=0 and session existed
        actual_session_present = session_present and not connect.flags.clean_session
        await self._send_connack(client, actual_session_present, ConnectReturnCode.ACCEPTED)
        
        logger.info(f"Client {client_id} connected (clean_session={connect.flags.clean_session}, session_present={actual_session_present})")
        
        # Send any pending messages from previous session
        if actual_session_present:
            await self._resend_pending_messages(client)
        
        return True
    
    async def _send_connack(self, client: ClientConnection, session_present: bool, 
                           return_code: ConnectReturnCode) -> None:
        """Send CONNACK packet."""
        packet = ConnackPacket(session_present=session_present, return_code=return_code).to_bytes()
        await self._send_packet(client, packet)
        
        if return_code != ConnectReturnCode.ACCEPTED:
            # Close connection after sending error CONNACK
            await self._close_connection(client, publish_will=False)
    
    async def _handle_packet(self, client: ClientConnection, packet_type: PacketType, 
                            flags: int, payload: bytes) -> None:
        """
        Handle incoming packet after CONNECT.
        """
        try:
            # Parse packet into appropriate packet object
            packet = self.factory.construct_packet(packet_type, flags, payload)
            
            if packet is None:
                logger.warning(f"Unhandled packet type {packet_type} from {client.client_id}")
                raise ProtocolError(f"Unsupported packet type: {packet_type.name}")

            await packet.handle(self, client)
            
        except ProtocolError as e:
            logger.warning(f"Invalid {packet_type.name} from {client.client_id}: {e}")
            await self._close_connection(client)

    
    async def _forward_publish(self, topic: str, payload: bytes, qos: int, retain: bool) -> None:
        """Forward a published message to all matching subscribers."""
        for client_id, client in list(self.clients.items()):
            if not client.connected or not client.session:
                continue
            
            # Check if any subscription matches
            matching_qos = client.session.get_matching_qos(topic, TopicUtils.topic_matches_filter)
            if matching_qos is not None:
                # Use minimum of publish QoS and subscription QoS
                effective_qos = min(qos, matching_qos)
                await self._send_publish(
                    client, topic, payload, effective_qos, retain=False
                )
    
    async def _send_publish(self, client: ClientConnection, topic: str, payload: bytes,
                           qos: int, retain: bool = False, dup: bool = False,
                           packet_id: Optional[int] = None) -> None:
        """Send PUBLISH packet to client."""
        if qos > 0:
            assert client.session is not None
            if packet_id is None:
                packet_id = client.session.get_next_packet_id()
            
            # Store for potential retransmission
            client.session.pending_outgoing[packet_id] = PendingMessage(
                packet_id=packet_id,
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                timestamp=time.time()
            )
        
        packet = PublishPacket(topic=topic, payload=payload, qos=qos, retain=retain, dup=dup, packet_id=packet_id).to_bytes()
        await self._send_packet(client, packet)
    
    async def _send_packet(self, client: ClientConnection, packet: bytes) -> None:
        """Send a packet to client."""
        try:
            client.writer.write(packet)
            await client.writer.drain()
        except (ConnectionResetError, BrokenPipeError) as e:
            logger.warning(f"Failed to send packet to {client.client_id}: {e}")
            client.connected = False
    
    async def _resend_pending_messages(self, client: ClientConnection) -> None:
        """Resend pending messages from previous session."""
        assert client.session is not None
        for packet_id, pending in list(client.session.pending_outgoing.items()):
            pending.retry_count += 1
            
            if pending.state == "pubrec_received":
                # Was waiting for PUBCOMP, resend PUBREL
                await self._send_packet(client, PubrelPacket(packet_id).to_bytes())
            else:
                # Resend PUBLISH with DUP=1
                await self._send_publish(
                    client, pending.topic, pending.payload, pending.qos,
                    pending.retain, dup=True, packet_id=packet_id
                )
    
    async def _monitor_keepalive(self, client: ClientConnection) -> None:
        """Monitor client keep-alive timeout."""
        # Timeout is 1.5 * keep_alive per spec
        timeout = client.keep_alive * 1.5
        
        while client.connected:
            await asyncio.sleep(1)
            
            if time.time() - client.last_activity > timeout:
                logger.warning(f"Keep-alive timeout for {client.client_id}")
                await self._close_connection(client, publish_will=True)
                break
    
    async def _close_connection(self, client: ClientConnection, publish_will: bool = False) -> None:
        """Close client connection."""
        client.connected = False
        
        # Publish will message if needed
        if publish_will and client.will_message:
            will = client.will_message
            await self._forward_publish(will.topic, will.payload, will.qos, will.retain)
            if will.retain:
                self.topic_manager.set_retained_message(will.topic, will.payload, will.qos)
            logger.info(f"Published will message for {client.client_id}")
        
        # Remove from active clients
        if client.client_id and client.client_id in self.clients:
            del self.clients[client.client_id]
        
        # Clean up session if clean_session was set
        if client.clean_session and client.client_id:
            self.session_manager.remove_session(client.client_id)
        
        # Close the connection
        try:
            client.writer.close()
            await client.writer.wait_closed()
        except Exception:
            pass
        
        if client.client_id:
            logger.info(f"Client {client.client_id} disconnected")


async def run_broker(host: str = "0.0.0.0", port: int = 1883, config_path: Optional[str] = None) -> None:
    """Run the MQTT broker."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    broker = MQTTBroker(host=host, port=port, config_path=config_path)
    
    try:
        await broker.start()
    except KeyboardInterrupt:
        pass
    finally:
        await broker.stop()
