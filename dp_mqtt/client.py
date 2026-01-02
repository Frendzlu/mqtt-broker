"""
MQTT 3.1.1 Client Implementation
Provides a paho-mqtt-like API for connecting to MQTT brokers.
"""

import asyncio
import logging
import os
import struct
import time
from datetime import datetime
from typing import Optional, Callable, Dict, List, Tuple, Any, Union
from dataclasses import dataclass, field
from enum import IntEnum

from .protocol import (
    PacketType, ConnectReturnCode, ProtocolError, MalformedPacketError,
    encode_string, encode_remaining_length, decode_string, decode_remaining_length,
    build_publish, build_puback, build_pubrec, build_pubrel, build_pubcomp,
    parse_publish,
)


logger = logging.getLogger(__name__)


def setup_client_logging(
    debug: bool = False,
    log_dir: str = "logs",
    prefix: str = "client"
) -> str:
    """
    Configure logging to both console and timestamped log file for clients.
    
    Args:
        debug: Enable debug-level logging
        log_dir: Directory for log files
        prefix: Prefix for log file name
        
    Returns:
        Path to the log file
    """
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{prefix}_{timestamp}.log")
    
    level = logging.DEBUG if debug else logging.INFO
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return log_file


class MQTTError(Exception):
    """Base MQTT client error."""
    pass


class ConnectionError(MQTTError):
    """Connection-related error."""
    pass


class MQTTMessage:
    """Represents a received MQTT message."""
    
    def __init__(self, topic: str, payload: bytes, qos: int, retain: bool):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
        self.mid: int = 0
        self.timestamp: float = time.time()
    
    @property
    def payload_str(self) -> str:
        """Return payload as UTF-8 string."""
        return self.payload.decode('utf-8', errors='replace')
    
    @property
    def timestamp_str(self) -> str:
        """Return timestamp as formatted string."""
        return datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    def __repr__(self) -> str:
        return f"MQTTMessage(topic={self.topic!r}, payload={self.payload!r}, qos={self.qos}, timestamp={self.timestamp})"
    
    def __str__(self) -> str:
        return f"[{self.timestamp_str}] Topic: {self.topic}, QoS: {self.qos}, Retain: {self.retain}, Payload: {self.payload_str}"


@dataclass
class MQTTClientConfig:
    """Client configuration."""
    client_id: str = ""
    clean_session: bool = True
    keep_alive: int = 60
    username: Optional[str] = None
    password: Optional[str] = None
    will_topic: Optional[str] = None
    will_payload: bytes = b""
    will_qos: int = 0
    will_retain: bool = False


class Client:
    """
    MQTT Client with paho-mqtt-like API.
    
    Example:
        client = Client("my-client")
        client.on_connect = lambda c, rc: print(f"Connected with code {rc}")
        client.on_message = lambda c, msg: print(f"Received: {msg.topic} = {msg.payload_str}")
        
        await client.connect("localhost", 1883)
        await client.subscribe("test/#")
        await client.publish("test/hello", "world")
        await client.loop_forever()
    """
    
    def __init__(self, client_id: str = "", clean_session: bool = True):
        """
        Initialize MQTT client.
        
        Args:
            client_id: Unique client identifier. If empty, broker assigns one.
            clean_session: If True, broker discards previous session state.
        """
        self.config = MQTTClientConfig(
            client_id=client_id,
            clean_session=clean_session
        )
        
        # Connection state
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._running = False
        
        # Packet ID management
        self._next_packet_id = 1
        self._pending_acks: Dict[int, asyncio.Future] = {}
        
        # Subscrptions
        self._subscriptions: Dict[str, int] = {}  # topic_filter -> qos
        
        # QoS 2 state
        self._pending_qos2: Dict[int, MQTTMessage] = {}
        
        # Callbacks
        self.on_connect: Optional[Callable[['Client', int], None]] = None
        self.on_disconnect: Optional[Callable[['Client', Optional[Exception]], None]] = None
        self.on_message: Optional[Callable[['Client', MQTTMessage], None]] = None
        self.on_publish: Optional[Callable[['Client', int], None]] = None
        self.on_subscribe: Optional[Callable[['Client', int, List[int]], None]] = None
        self.on_unsubscribe: Optional[Callable[['Client', int], None]] = None
        
        # Background tasks
        self._read_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._last_activity = 0.0
    
    def username_pw_set(self, username: str, password: Optional[str] = None) -> None:
        """Set username and password for authentication."""
        self.config.username = username
        self.config.password = password
    
    def will_set(self, topic: str, payload: Union[str, bytes] = b"", 
                 qos: int = 0, retain: bool = False) -> None:
        """Set Last Will and Testament message."""
        self.config.will_topic = topic
        self.config.will_payload = payload.encode() if isinstance(payload, str) else payload
        self.config.will_qos = qos
        self.config.will_retain = retain
    
    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._connected
    
    async def connect(self, host: str = "localhost", port: int = 1883, 
                      keepalive: int = 60) -> int:
        """
        Connect to MQTT broker.
        
        Args:
            host: Broker hostname or IP address.
            port: Broker port (default 1883).
            keepalive: Keep-alive interval in seconds.
        
        Returns:
            Connection return code (0 = success).
        """
        self.config.keep_alive = keepalive
        
        try:
            self._reader, self._writer = await asyncio.open_connection(host, port)
        except OSError as e:
            raise ConnectionError(f"Failed to connect to {host}:{port}: {e}")
        
        # Build and send CONNECT packet
        connect_packet = self._build_connect()
        await self._send_packet(connect_packet)
        
        # Wait for CONNACK
        packet_type, flags, payload = await self._read_packet()
        
        if packet_type != PacketType.CONNACK:
            await self.disconnect()
            raise ProtocolError(f"Expected CONNACK, got {packet_type}")
        
        # Parse CONNACK
        session_present = bool(payload[0] & 0x01)
        return_code = payload[1]
        
        if return_code != 0:
            await self.disconnect()
            if self.on_connect:
                self.on_connect(self, return_code)
            return return_code
        
        self._connected = True
        self._last_activity = time.time()
        
        if self.on_connect:
            self.on_connect(self, return_code)
        
        logger.info(f"Connected to {host}:{port} (session_present={session_present})")
        return return_code
    
    async def disconnect(self) -> None:
        """Disconnect from broker gracefully."""
        if self._connected:
            try:
                # Send DISCONNECT packet
                disconnect_packet = bytes([0xE0, 0x00])  # DISCONNECT with 0 remaining length
                await self._send_packet(disconnect_packet)
            except Exception:
                pass
        
        self._connected = False
        self._running = False
        
        # Cancel background tasks
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
        
        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
        
        # Close connection
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
        
        self._reader = None
        self._writer = None
        
        if self.on_disconnect:
            self.on_disconnect(self, None)
        
        logger.info("Disconnected from broker")
    
    async def publish(self, topic: str, payload: Union[str, bytes] = b"",
                      qos: int = 0, retain: bool = False) -> int:
        """
        Publish a message.
        
        Args:
            topic: Topic to publish to.
            payload: Message payload.
            qos: Quality of Service level (0, 1, or 2).
            retain: Whether to retain the message.
        
        Returns:
            Message ID (packet_id for QoS > 0, 0 for QoS 0).
        """
        if not self._connected:
            raise MQTTError("Not connected")
        
        if isinstance(payload, str):
            payload = payload.encode('utf-8')
        
        packet_id = None
        if qos > 0:
            packet_id = self._get_next_packet_id()
        
        packet = build_publish(topic, payload, qos, retain, dup=False, packet_id=packet_id)
        await self._send_packet(packet)
        
        if qos == 0:
            if self.on_publish:
                self.on_publish(self, 0)
            return 0
        
        # Wait for acknowledgment
        assert packet_id is not None
        
        # If loop is running, use futures; otherwise read inline
        if self._running:
            future: asyncio.Future[int] = asyncio.get_event_loop().create_future()
            self._pending_acks[packet_id] = future
            
            try:
                await asyncio.wait_for(future, timeout=30.0)
                if self.on_publish:
                    self.on_publish(self, packet_id)
                return packet_id
            except asyncio.TimeoutError:
                del self._pending_acks[packet_id]
                raise MQTTError(f"Timeout waiting for PUBACK/PUBCOMP for packet {packet_id}")
        else:
            # Read response inline
            await self._wait_for_ack_inline(packet_id, qos)
            if self.on_publish:
                self.on_publish(self, packet_id)
            return packet_id
    
    async def subscribe(self, topic: Union[str, List[Tuple[str, int]]], 
                        qos: int = 0) -> Tuple[int, List[int]]:
        """
        Subscribe to topic(s).
        
        Args:
            topic: Topic filter string, or list of (topic, qos) tuples.
            qos: QoS level (only used if topic is string).
        
        Returns:
            Tuple of (message_id, list of granted QoS values).
        """
        if not self._connected:
            raise MQTTError("Not connected")
        
        # Normalize input
        if isinstance(topic, str):
            topics = [(topic, qos)]
        else:
            topics = topic
        
        packet_id = self._get_next_packet_id()
        
        # Build SUBSCRIBE packet
        packet = self._build_subscribe(packet_id, topics)
        await self._send_packet(packet)
        
        # Wait for SUBACK
        if self._running:
            future: asyncio.Future[List[int]] = asyncio.get_event_loop().create_future()
            self._pending_acks[packet_id] = future
            
            try:
                granted_qos = await asyncio.wait_for(future, timeout=30.0)
            except asyncio.TimeoutError:
                del self._pending_acks[packet_id]
                raise MQTTError(f"Timeout waiting for SUBACK for packet {packet_id}")
        else:
            # Read response inline
            granted_qos = await self._wait_for_suback_inline(packet_id)
        
        # Store subscriptions
        for (topic_filter, _), granted in zip(topics, granted_qos):
            if granted != 0x80:  # Not failure
                self._subscriptions[topic_filter] = granted
        
        if self.on_subscribe:
            self.on_subscribe(self, packet_id, granted_qos)
        
        return packet_id, granted_qos
    
    async def unsubscribe(self, topic: Union[str, List[str]]) -> int:
        """
        Unsubscribe from topic(s).
        
        Args:
            topic: Topic filter string, or list of topic filters.
        
        Returns:
            Message ID.
        """
        if not self._connected:
            raise MQTTError("Not connected")
        
        # Normalize input
        if isinstance(topic, str):
            topics = [topic]
        else:
            topics = topic
        
        packet_id = self._get_next_packet_id()
        
        # Build UNSUBSCRIBE packet
        packet = self._build_unsubscribe(packet_id, topics)
        await self._send_packet(packet)
        
        # Wait for UNSUBACK
        if self._running:
            future: asyncio.Future[int] = asyncio.get_event_loop().create_future()
            self._pending_acks[packet_id] = future
            
            try:
                await asyncio.wait_for(future, timeout=30.0)
            except asyncio.TimeoutError:
                del self._pending_acks[packet_id]
                raise MQTTError(f"Timeout waiting for UNSUBACK for packet {packet_id}")
        else:
            # Read response inline
            await self._wait_for_unsuback_inline(packet_id)
        
        # Remove subscriptions
        for topic_filter in topics:
            self._subscriptions.pop(topic_filter, None)
        
        if self.on_unsubscribe:
            self.on_unsubscribe(self, packet_id)
        
        return packet_id
    
    async def loop_forever(self) -> None:
        """
        Run the client loop until disconnected.
        Handles incoming packets and keep-alive.
        """
        if not self._connected:
            raise MQTTError("Not connected")
        
        self._running = True
        
        # Start background tasks
        self._read_task = asyncio.create_task(self._read_loop())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        
        try:
            await self._read_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in read loop: {e}")
            if self.on_disconnect:
                self.on_disconnect(self, e)
    
    async def loop_start(self) -> None:
        """Start the network loop in the background."""
        if not self._connected:
            raise MQTTError("Not connected")
        
        self._running = True
        self._read_task = asyncio.create_task(self._read_loop())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
    
    async def loop_stop(self) -> None:
        """Stop the network loop."""
        self._running = False
        
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
        
        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
    
    # -------------------------------------------------------------------------
    # Internal methods
    # -------------------------------------------------------------------------
    
    def _get_next_packet_id(self) -> int:
        """Get next available packet ID."""
        packet_id = self._next_packet_id
        self._next_packet_id = (self._next_packet_id % 65535) + 1
        return packet_id
    
    async def _wait_for_ack_inline(self, packet_id: int, qos: int) -> None:
        """Wait for PUBACK or PUBCOMP inline (when loop is not running)."""
        import struct
        
        if qos == 1:
            # Wait for PUBACK
            while True:
                ptype, flags, payload = await self._read_packet()
                if ptype == PacketType.PUBACK:
                    recv_id = struct.unpack("!H", payload[:2])[0]
                    if recv_id == packet_id:
                        return
        elif qos == 2:
            # Wait for PUBREC -> send PUBREL -> wait for PUBCOMP
            while True:
                ptype, flags, payload = await self._read_packet()
                if ptype == PacketType.PUBREC:
                    recv_id = struct.unpack("!H", payload[:2])[0]
                    if recv_id == packet_id:
                        await self._send_packet(build_pubrel(packet_id))
                        break
            
            # Wait for PUBCOMP
            while True:
                ptype, flags, payload = await self._read_packet()
                if ptype == PacketType.PUBCOMP:
                    recv_id = struct.unpack("!H", payload[:2])[0]
                    if recv_id == packet_id:
                        return
    
    async def _wait_for_suback_inline(self, packet_id: int) -> List[int]:
        """Wait for SUBACK inline (when loop is not running)."""
        import struct
        
        while True:
            ptype, flags, payload = await self._read_packet()
            if ptype == PacketType.SUBACK:
                recv_id = struct.unpack("!H", payload[:2])[0]
                if recv_id == packet_id:
                    return list(payload[2:])
    
    async def _wait_for_unsuback_inline(self, packet_id: int) -> None:
        """Wait for UNSUBACK inline (when loop is not running)."""
        import struct
        
        while True:
            ptype, flags, payload = await self._read_packet()
            if ptype == PacketType.UNSUBACK:
                recv_id = struct.unpack("!H", payload[:2])[0]
                if recv_id == packet_id:
                    return
    
    async def _send_packet(self, packet: bytes) -> None:
        """Send packet to broker."""
        if not self._writer:
            raise MQTTError("Not connected")
        
        self._writer.write(packet)
        await self._writer.drain()
        self._last_activity = time.time()
    
    async def _read_packet(self) -> Tuple[PacketType, int, bytes]:
        """Read a complete MQTT packet from the connection."""
        if not self._reader:
            raise MQTTError("Not connected")
        
        # Read first byte (packet type + flags)
        first_byte = await self._reader.read(1)
        if not first_byte:
            raise ConnectionError("Connection closed by broker")
        
        packet_type = PacketType(first_byte[0] >> 4)
        flags = first_byte[0] & 0x0F
        
        # Read remaining length
        remaining_length = 0
        multiplier = 1
        for _ in range(4):
            byte = await self._reader.read(1)
            if not byte:
                raise ConnectionError("Connection closed while reading remaining length")
            remaining_length += (byte[0] & 0x7F) * multiplier
            multiplier *= 128
            if (byte[0] & 0x80) == 0:
                break
        
        # Read payload
        payload = b""
        if remaining_length > 0:
            payload = await self._reader.readexactly(remaining_length)
        
        self._last_activity = time.time()
        return packet_type, flags, payload
    
    async def _read_loop(self) -> None:
        """Background task to read incoming packets."""
        try:
            while self._running and self._connected:
                try:
                    packet_type, flags, payload = await self._read_packet()
                    await self._handle_packet(packet_type, flags, payload)
                except asyncio.IncompleteReadError:
                    break
                except ConnectionError:
                    break
        except Exception as e:
            logger.error(f"Read loop error: {e}")
        finally:
            self._connected = False
    
    async def _keepalive_loop(self) -> None:
        """Background task to send PINGREQ."""
        try:
            while self._running and self._connected:
                await asyncio.sleep(self.config.keep_alive / 2)
                
                if time.time() - self._last_activity > self.config.keep_alive / 2:
                    # Send PINGREQ
                    pingreq = bytes([0xC0, 0x00])
                    await self._send_packet(pingreq)
                    logger.debug("Sent PINGREQ")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Keepalive loop error: {e}")
    
    async def _handle_packet(self, packet_type: PacketType, flags: int, payload: bytes) -> None:
        """Handle incoming packet."""
        handlers = {
            PacketType.PUBLISH: self._handle_publish,
            PacketType.PUBACK: self._handle_puback,
            PacketType.PUBREC: self._handle_pubrec,
            PacketType.PUBREL: self._handle_pubrel,
            PacketType.PUBCOMP: self._handle_pubcomp,
            PacketType.SUBACK: self._handle_suback,
            PacketType.UNSUBACK: self._handle_unsuback,
            PacketType.PINGRESP: self._handle_pingresp,
        }
        
        handler = handlers.get(packet_type)
        if handler:
            await handler(flags, payload)
        else:
            logger.warning(f"Unhandled packet type: {packet_type}")
    
    async def _handle_publish(self, flags: int, payload: bytes) -> None:
        """Handle incoming PUBLISH packet."""
        publish = parse_publish(flags, payload)
        
        msg = MQTTMessage(
            topic=publish.topic,
            payload=publish.payload,
            qos=publish.qos,
            retain=publish.retain
        )
        
        if publish.packet_id:
            msg.mid = publish.packet_id
        
        if publish.qos == 1:
            # Send PUBACK
            assert publish.packet_id is not None
            await self._send_packet(build_puback(publish.packet_id))
            if self.on_message:
                self.on_message(self, msg)
        
        elif publish.qos == 2:
            # Send PUBREC, store message for later delivery
            assert publish.packet_id is not None
            self._pending_qos2[publish.packet_id] = msg
            await self._send_packet(build_pubrec(publish.packet_id))
        
        else:  # QoS 0
            if self.on_message:
                self.on_message(self, msg)
    
    async def _handle_puback(self, flags: int, payload: bytes) -> None:
        """Handle PUBACK (QoS 1 acknowledgment)."""
        packet_id = struct.unpack("!H", payload[:2])[0]
        
        if packet_id in self._pending_acks:
            future = self._pending_acks.pop(packet_id)
            if not future.done():
                future.set_result(packet_id)
    
    async def _handle_pubrec(self, flags: int, payload: bytes) -> None:
        """Handle PUBREC (QoS 2 step 2)."""
        packet_id = struct.unpack("!H", payload[:2])[0]
        
        # Send PUBREL
        await self._send_packet(build_pubrel(packet_id))
    
    async def _handle_pubrel(self, flags: int, payload: bytes) -> None:
        """Handle PUBREL (QoS 2 step 3)."""
        packet_id = struct.unpack("!H", payload[:2])[0]
        
        # Send PUBCOMP
        await self._send_packet(build_pubcomp(packet_id))
        
        # Deliver message
        if packet_id in self._pending_qos2:
            msg = self._pending_qos2.pop(packet_id)
            if self.on_message:
                self.on_message(self, msg)
    
    async def _handle_pubcomp(self, flags: int, payload: bytes) -> None:
        """Handle PUBCOMP (QoS 2 complete)."""
        packet_id = struct.unpack("!H", payload[:2])[0]
        
        if packet_id in self._pending_acks:
            future = self._pending_acks.pop(packet_id)
            if not future.done():
                future.set_result(packet_id)
    
    async def _handle_suback(self, flags: int, payload: bytes) -> None:
        """Handle SUBACK."""
        packet_id = struct.unpack("!H", payload[:2])[0]
        granted_qos = list(payload[2:])
        
        if packet_id in self._pending_acks:
            future = self._pending_acks.pop(packet_id)
            if not future.done():
                future.set_result(granted_qos)
    
    async def _handle_unsuback(self, flags: int, payload: bytes) -> None:
        """Handle UNSUBACK."""
        packet_id = struct.unpack("!H", payload[:2])[0]
        
        if packet_id in self._pending_acks:
            future = self._pending_acks.pop(packet_id)
            if not future.done():
                future.set_result(packet_id)
    
    async def _handle_pingresp(self, flags: int, payload: bytes) -> None:
        """Handle PINGRESP."""
        logger.debug("Received PINGRESP")
    
    def _build_connect(self) -> bytes:
        """Build CONNECT packet."""
        # Variable header
        variable_header = bytearray()
        
        # Protocol name "MQTT"
        variable_header.extend(encode_string("MQTT"))
        
        # Protocol level (4 for MQTT 3.1.1)
        variable_header.append(0x04)
        
        # Connect flags
        connect_flags = 0
        if self.config.clean_session:
            connect_flags |= 0x02
        if self.config.will_topic:
            connect_flags |= 0x04  # Will flag
            connect_flags |= (self.config.will_qos & 0x03) << 3
            if self.config.will_retain:
                connect_flags |= 0x20
        if self.config.password:
            connect_flags |= 0x40
        if self.config.username:
            connect_flags |= 0x80
        
        variable_header.append(connect_flags)
        
        # Keep alive
        variable_header.extend(struct.pack("!H", self.config.keep_alive))
        
        # Payload
        payload = bytearray()
        
        # Client ID
        payload.extend(encode_string(self.config.client_id))
        
        # Will topic and message
        if self.config.will_topic:
            payload.extend(encode_string(self.config.will_topic))
            payload.extend(struct.pack("!H", len(self.config.will_payload)))
            payload.extend(self.config.will_payload)
        
        # Username
        if self.config.username:
            payload.extend(encode_string(self.config.username))
        
        # Password
        if self.config.password:
            password_bytes = self.config.password.encode('utf-8')
            payload.extend(struct.pack("!H", len(password_bytes)))
            payload.extend(password_bytes)
        
        # Fixed header
        remaining_length = len(variable_header) + len(payload)
        fixed_header = bytes([0x10]) + encode_remaining_length(remaining_length)
        
        return fixed_header + bytes(variable_header) + bytes(payload)
    
    def _build_subscribe(self, packet_id: int, topics: List[Tuple[str, int]]) -> bytes:
        """Build SUBSCRIBE packet."""
        # Variable header
        variable_header = struct.pack("!H", packet_id)
        
        # Payload
        payload = bytearray()
        for topic_filter, qos in topics:
            payload.extend(encode_string(topic_filter))
            payload.append(qos & 0x03)
        
        # Fixed header (SUBSCRIBE = 0x82)
        remaining_length = len(variable_header) + len(payload)
        fixed_header = bytes([0x82]) + encode_remaining_length(remaining_length)
        
        return fixed_header + variable_header + bytes(payload)
    
    def _build_unsubscribe(self, packet_id: int, topics: List[str]) -> bytes:
        """Build UNSUBSCRIBE packet."""
        # Variable header
        variable_header = struct.pack("!H", packet_id)
        
        # Payload
        payload = bytearray()
        for topic_filter in topics:
            payload.extend(encode_string(topic_filter))
        
        # Fixed header (UNSUBSCRIBE = 0xA2)
        remaining_length = len(variable_header) + len(payload)
        fixed_header = bytes([0xA2]) + encode_remaining_length(remaining_length)
        
        return fixed_header + variable_header + bytes(payload)
