# MQTT 3.1.1 Broker

A Python implementation of an MQTT 3.1.1 broker.

## Features

- Full MQTT 3.1.1 protocol support
- QoS 0, 1, and 2 message delivery
- Retained messages
- Will messages
- Session persistence (clean_session flag)
- Topic wildcards (+ and #)
- Keep-alive monitoring

## Installation

```bash
cd mqtt-broker
pip install -e .
```

## Usage

### Running the Broker

```bash
# Using the module
python -m mqtt_broker

# With custom host and port
python -m mqtt_broker --host 0.0.0.0 --port 1883

# With debug logging
python -m mqtt_broker --debug
```

### Using in Code

```python
import asyncio
from mqtt_broker import MQTTBroker

async def main():
    broker = MQTTBroker(host="0.0.0.0", port=1883)
    await broker.start()

asyncio.run(main())
```

## Testing with an MQTT Client

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
├── protocol.py      # MQTT packet parsing/building
├── session.py       # Client session management
└── topics.py        # Topic matching and retained messages
```

## License

MIT
