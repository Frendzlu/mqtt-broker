# MQTT Examples

Example scripts demonstrating the MQTT client usage.

## Running the Examples

First, start the broker:

```bash
python -m dp_mqtt
```

Then, in separate terminals, run the examples:

### Publisher Example
```bash
python examples/publisher.py
```

### Subscriber Example  
```bash
python examples/subscriber.py
```

### Complete Demo
```bash
python examples/demo.py
```

## Usage Patterns

### Basic Publish/Subscribe

```python
import asyncio
from dp_mqtt import Client

async def main():
    client = Client("my-client")
    
    # Set message callback
    client.on_message = lambda c, msg: print(f"{msg.topic}: {msg.payload_str}")
    
    # Connect
    await client.connect("localhost", 1883)
    
    # Subscribe
    await client.subscribe("my/topic/#")
    
    # Publish
    await client.publish("my/topic/test", "Hello!")
    
    # Run forever
    await client.loop_forever()

asyncio.run(main())
```

### With Authentication

```python
client = Client("my-client")
client.username_pw_set("username", "password")
await client.connect("localhost", 1883)
```

### With Last Will

```python
client = Client("my-client")
client.will_set("status/my-client", "offline", qos=1, retain=True)
await client.connect("localhost", 1883)
await client.publish("status/my-client", "online", qos=1, retain=True)
```

### QoS Levels

```python
# QoS 0 - At most once (fire and forget)
await client.publish("topic", "data", qos=0)

# QoS 1 - At least once (acknowledged)
await client.publish("topic", "data", qos=1)

# QoS 2 - Exactly once (4-way handshake)
await client.publish("topic", "data", qos=2)
```

### Multiple Subscriptions

```python
await client.subscribe([
    ("sensors/+/temperature", 1),
    ("alerts/#", 2),
    ("status/online", 0),
])
```
