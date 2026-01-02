# dp-mqtt

A Python implementation of an MQTT 3.1.1 broker with a built-in async client library.

## Features

- Full MQTT 3.1.1 protocol support
- QoS 0, 1, and 2 message delivery
- Retained messages
- Will messages
- Session persistence (clean_session flag)
- Topic wildcards (+ and #)
- Keep-alive monitoring
- **Built-in async client** (paho-mqtt-like API)

## Installation

```bash
cd mqtt-broker
pip install -e .
```

## Usage

### Running the Broker

```bash
# Using the module
python -m dp_mqtt

# With custom host and port
python -m dp_mqtt --host 0.0.0.0 --port 1883

# With debug logging
python -m dp_mqtt --debug
```

### Using the Built-in Client

```python
import asyncio
from dp_mqtt import Client, MQTTMessage

async def main():
    # Create client
    client = Client(client_id="my-client")
    
    # Set up callbacks
    def on_message(client: Client, msg: MQTTMessage):
        print(f"{msg.topic}: {msg.payload_str}")
    
    client.on_message = on_message
    
    # Connect, subscribe, publish
    await client.connect("localhost", 1883)
    await client.subscribe("test/#", qos=1)
    await client.publish("test/hello", "world", qos=1)
    
    # Run event loop
    await client.loop_forever()

asyncio.run(main())
```

### Client Features

```python
# Authentication
client.username_pw_set("user", "password")

# Last Will and Testament
client.will_set("status/client", "offline", qos=1, retain=True)

# Multiple subscriptions
await client.subscribe([
    ("sensors/#", 1),
    ("alerts/+", 2),
])

# QoS levels
await client.publish("topic", "data", qos=0)  # At most once
await client.publish("topic", "data", qos=1)  # At least once
await client.publish("topic", "data", qos=2)  # Exactly once
```

### Using the Broker in Code

```python
import asyncio
from dp_mqtt import MQTTBroker

async def main():
    broker = MQTTBroker(host="0.0.0.0", port=1883)
    await broker.start()

asyncio.run(main())
```

## Examples

See the `examples/` directory for complete examples:

- `examples/publisher.py` - Publish messages with different QoS levels
- `examples/subscriber.py` - Subscribe and receive messages
- `examples/demo.py` - Complete client workflow demo

## Testing with External MQTT Clients

You can test the broker using any MQTT client like `mosquitto_pub` and `mosquitto_sub`:

```bash
# Subscribe to a topic
mosquitto_sub -h localhost -t "test/topic"

# Publish to a topic
mosquitto_pub -h localhost -t "test/topic" -m "Hello, MQTT!"
```

Or using Python with paho-mqtt:

```python
import paho.mqtt.client as mqtt

def on_message(client, userdata, msg):
    print(f"{msg.topic}: {msg.payload.decode()}")

client = mqtt.Client()
client.on_message = on_message
client.connect("localhost", 1883)
client.subscribe("test/#")
client.loop_forever()
```

## Protocol Compliance

This implementation follows the MQTT 3.1.1 specification with the following features:

### Packet Types Supported
- CONNECT / CONNACK
- PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP
- SUBSCRIBE / SUBACK
- UNSUBSCRIBE / UNSUBACK
- PINGREQ / PINGRESP
- DISCONNECT

### String Handling
- UTF-8 encoding with 2-byte length prefix
- U+0000 not allowed in strings
- BOM (0xEF 0xBB 0xBF) treated as U+FEFF

### Topic Matching
- Single-level wildcard (+) matches any single level
- Multi-level wildcard (#) must be at end, matches all remaining levels
- Topics starting with $ are system topics (don't match leading wildcards)

### Connection Handling
- Protocol name validation (must be "MQTT")
- Protocol level validation (must be 4 for MQTT 3.1.1)
- Reserved flag validation
- Keep-alive timeout (1.5x keep-alive value)
- Will message publishing on unexpected disconnect

## Project Structure

```
mqtt_broker/
├── __init__.py      # Package exports
├── __main__.py      # CLI entry point
├── broker.py        # Main broker logic
├── client.py        # Async MQTT client
├── protocol.py      # MQTT packet parsing/building
├── session.py       # Client session management
└── topics.py        # Topic matching and retained messages

examples/
├── publisher.py     # Publisher example
├── subscriber.py    # Subscriber example
├── demo.py          # Complete demo
└── README.md        # Examples documentation
```

## Design Patterns

This project implements several classic design patterns to achieve clean, maintainable, and extensible code.

### 1. Observer Pattern

**Location:** `client.py` - Client callbacks

The client uses the Observer pattern to notify subscribers about events through callbacks. Users register callback functions that are invoked when specific events occur.

```python
from dp_mqtt import Client, MQTTMessage

client = Client("my-client")

# Register observers (callbacks)
def on_connect(client: Client, rc: int):
    print(f"Connected with code {rc}")

def on_message(client: Client, msg: MQTTMessage):
    print(f"Received: {msg.topic} = {msg.payload_str}")

client.on_connect = on_connect    # Subscribe to connect events
client.on_message = on_message    # Subscribe to message events
client.on_publish = lambda c, mid: print(f"Published {mid}")
client.on_subscribe = lambda c, mid, qos: print(f"Subscribed")
```

**Benefits:** Decouples event producers from consumers, allows multiple listeners, enables reactive programming.

---

### 2. Command Pattern

**Location:** `broker.py` - Packet handlers

The broker maps packet types to handler methods, encapsulating each operation as a command.

```python
# Internal implementation in MQTTBroker._handle_packet()
handlers = {
    PacketType.PUBLISH: self._handle_publish,
    PacketType.PUBACK: self._handle_puback,
    PacketType.SUBSCRIBE: self._handle_subscribe,
    PacketType.UNSUBSCRIBE: self._handle_unsubscribe,
    PacketType.PINGREQ: self._handle_pingreq,
    PacketType.DISCONNECT: self._handle_disconnect,
}

handler = handlers.get(packet_type)
if handler:
    await handler(client, flags, payload)
```

**Benefits:** Easy to add new packet types, separates packet routing from handling logic, simplifies testing individual handlers.

---

### 3. Registry / Repository Pattern

**Location:** `session.py` - SessionManager, `topics.py` - TopicManager

Centralized registries manage collections of domain objects with consistent access patterns.

```python
from dp_mqtt.session import SessionManager
from dp_mqtt.topics import TopicManager

# SessionManager - registry of client sessions
session_manager = SessionManager()
session, existed = session_manager.get_or_create_session("client-1", clean_session=True)
session_manager.has_session("client-1")  # True
session_manager.remove_session("client-1")

# TopicManager - registry of retained messages
topic_manager = TopicManager()
topic_manager.set_retained_message("sensors/temp", b"22.5", qos=1)
msg = topic_manager.get_retained_message("sensors/temp")
matching = topic_manager.get_matching_retained_messages("sensors/#", topic_matches_filter)
```

**Benefits:** Single source of truth, encapsulates storage logic, provides consistent CRUD operations.

---

### 4. Data Transfer Object (DTO) / Value Object

**Location:** All modules using `@dataclass`

Immutable or simple data containers transfer data between layers without business logic.

```python
from dataclasses import dataclass

@dataclass
class WillMessage:
    """Client's Last Will message."""
    topic: str
    payload: bytes
    qos: int
    retain: bool

@dataclass  
class RetainedMessage:
    """A retained message for a topic."""
    topic: str
    payload: bytes
    qos: int
    timestamp: float

@dataclass
class PendingMessage:
    """Message awaiting acknowledgment."""
    packet_id: int
    topic: str
    payload: bytes
    qos: int
    retain: bool
    timestamp: float
    retry_count: int = 0
    state: str = "pending"
```

**Benefits:** Clear data contracts, type safety, reduced boilerplate with dataclasses.

---

### 5. Facade Pattern

**Location:** `broker.py` - MQTTBroker, `client.py` - Client

High-level classes provide simplified interfaces to complex subsystems.

```python
from dp_mqtt import MQTTBroker, Client

# MQTTBroker facade hides: protocol parsing, session management, 
# topic matching, QoS handling, keep-alive monitoring
broker = MQTTBroker(host="0.0.0.0", port=1883)
await broker.start()  # Simple API, complex internals

# Client facade hides: packet building, connection state,
# acknowledgment tracking, background tasks
client = Client("my-client")
await client.connect("localhost", 1883)
await client.publish("topic", "message", qos=2)  # Handles full QoS 2 flow internally
```

**Benefits:** Reduces complexity for users, hides implementation details, provides clean API.

---

### 6. State Pattern

**Location:** `session.py` - PendingMessage.state

Messages transition through different states during QoS 2 delivery.

```python
@dataclass
class PendingMessage:
    # ...
    state: str = "pending"  # States: "pending" -> "pubrec_received"

# QoS 2 state transitions:
# 1. PUBLISH sent     -> state = "pending"
# 2. PUBREC received  -> state = "pubrec_received", send PUBREL
# 3. PUBCOMP received -> message removed (complete)
```

**Benefits:** Clean state transitions, explicit state tracking, easier debugging of QoS flows.

---

### 7. Template Method Pattern

**Location:** `broker.py` - Client handling flow

The `_handle_client` method defines a skeleton algorithm with steps delegated to specialized methods.

```python
async def _handle_client(self, reader, writer):
    # Template: defines the structure
    client = ClientConnection(reader=reader, writer=writer)
    
    # Step 1: Wait for CONNECT (delegated)
    first_packet = await self._read_packet(client)
    
    # Step 2: Handle connection (delegated)
    if not await self._handle_connect(client, flags, payload):
        return
    
    # Step 3: Main loop - process packets (delegated)
    while self.running and client.connected:
        packet = await self._read_packet(client)
        await self._handle_packet(client, packet_type, flags, payload)
    
    # Step 4: Cleanup (delegated)
    await self._close_connection(client)
```

**Benefits:** Reusable algorithm structure, customizable steps, consistent flow handling.

---

### 8. Strategy Pattern

**Location:** `topics.py` - Topic matching function

The `topic_matches_filter` function can be passed as a strategy to other components.

```python
from dp_mqtt.topics import topic_matches_filter

# Strategy is injected into Session
class Session:
    def get_matching_qos(self, topic: str, topic_matches_filter) -> Optional[int]:
        for filter_pattern, qos in self.subscriptions.items():
            if topic_matches_filter(topic, filter_pattern):  # Strategy call
                if matching_qos is None or qos > matching_qos:
                    matching_qos = qos
        return matching_qos

# Usage - strategy is passed in
matching_qos = session.get_matching_qos("sensors/temp", topic_matches_filter)

# Could use different matching strategy:
# matching_qos = session.get_matching_qos("sensors/temp", custom_matcher)
```

**Benefits:** Interchangeable algorithms, easier testing with mock strategies, flexible behavior.

---

### Pattern Summary

| Pattern | Location | Purpose |
|---------|----------|---------|
| **Observer** | `Client` callbacks | Event notification system |
| **Command** | `MQTTBroker` handlers | Packet type routing |
| **Registry** | `SessionManager`, `TopicManager` | Centralized object management |
| **DTO** | `@dataclass` types | Data transfer containers |
| **Facade** | `MQTTBroker`, `Client` | Simplified API |
| **State** | `PendingMessage.state` | QoS 2 flow states |
| **Template Method** | `_handle_client` | Client handling flow |
| **Strategy** | `topic_matches_filter` | Pluggable matching algorithm |

## License

MIT
