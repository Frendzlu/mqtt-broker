# dp-mqtt - MQTT 3.1.1 Broker

A lightweight, pure Python MQTT 3.1.1 broker with authentication support.

## Features

- Full MQTT 3.1.1 protocol support
- QoS 0, 1, and 2
- Retained messages
- Will messages
- Username/password authentication
- Clean session and persistent sessions
- Dockerized deployment
- YAML configuration

## Quick Start

### 1. Clone and Setup

```bash
git clone <your-repo-url>
cd mqtt-broker

# Install the package
pip install -e .

# Or install with dev dependencies
pip install -e ".[dev]"
```

### 2. Configure

Edit `config.yaml` in the project root:

```yaml
authentication:
  allow_anonymous: false  # Set to true to allow connections without credentials
  
  users:
    - username: admin
      password: sha256:8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918
      # Password: admin

broker:
  port: 1883        # MQTT port - MUST match MQTT_PORT in .env for Docker!
  host: 0.0.0.0     # Bind address
  max_qos: 2        # Maximum QoS level supported
```

**To change the MQTT port:**
1. Edit `broker.port` in `config.yaml`
2. Edit `MQTT_PORT` in `.env` to match
3. Restart: `docker-compose restart`

**Generate password hash:**
```bash
python -c "from dp_mqtt.auth import AuthManager; print(AuthManager.generate_password_hash('your_password'))"
```

### 3. Run the Broker

#### Option A: Docker (Recommended)

```bash
# Start broker
docker-compose up -d

# View logs
docker logs -f dp-mqtt-broker

# Stop broker
docker-compose down
```

The broker will:
- Read configuration from `config.yaml`
- Use the port specified in config (default: 1883)
- Require authentication with configured users

#### Option B: Direct Python

```bash
python -m dp_mqtt --config config.yaml
```

### 4. Connect Clients

#### Using dp_mqtt Client

```bash
python client_example_dp.py
```

Or in your own code:

```python
import asyncio
from dp_mqtt import Client

async def main():
    client = Client(client_id="my-client")
    client.username_pw_set("admin", "admin")  # Use credentials from config.yaml
    
    # Connect
    rc = await client.connect("localhost", 1883)
    if rc == 0:
        print("Connected!")
        
        # Subscribe
        await client.subscribe([("test/topic", 1)])
        
        # Publish
        await client.publish("test/topic", "Hello MQTT!", qos=1)
        
        # Wait a bit for messages
        await asyncio.sleep(1)
        
        # Disconnect
        await client.disconnect()

asyncio.run(main())
```

#### Using paho-mqtt Client

```bash
pip install paho-mqtt
python client_example_paho.py
```

Or in your own code:

```python
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

client = mqtt.Client(
    client_id="paho-client",
    protocol=mqtt.MQTTv311,
    callback_api_version=CallbackAPIVersion.VERSION2
)

# Set authentication
client.username_pw_set("admin", "admin")

# Connect and use
client.connect("localhost", 1883, keepalive=60)
client.loop_start()

client.publish("test/topic", "Hello from paho!")

client.loop_stop()
client.disconnect()
```

## Configuration File Structure

The `config.yaml` file **must** be present in the project root (or specify path with `--config`):

```yaml
# Authentication settings
authentication:
  # Allow connections without username/password
  allow_anonymous: false
  
  # List of users
  users:
    - username: user1
      password: sha256:hash_here  # Hashed password
    - username: user2
      password: plaintext_pass    # Will show warning, use hashed!

# Broker settings
broker:
  port: 1883      # Port to listen on (used by Docker)
  host: 0.0.0.0   # Interface to bind to
  max_qos: 2      # Maximum QoS level (0, 1, or 2)
```

### Default Credentials

The included `config.yaml` has following users:

| Username | Password |
|----------|----------|
| admin | admin |
| iot_device | password |
| publisher | publisher123 |
| subscriber | subscriber456 |

## Project Structure

```
mqtt-broker/
├── config.yaml              # Main configuration (REQUIRED)
├── docker-compose.yml       # Docker orchestration
├── Dockerfile              # Container image
├── client_example_dp.py    # Example using dp_mqtt client
├── client_example_paho.py  # Example using paho-mqtt
├── dp_mqtt/                # Main package
│   ├── __init__.py
│   ├── __main__.py         # CLI entry point
│   ├── broker.py           # Broker implementation
│   ├── client.py           # Async client
│   ├── protocol.py         # MQTT protocol
│   ├── auth.py             # Authentication
│   ├── session.py          # Session management
│   └── topics.py           # Topic management
├── tests/                  # Test suite
└── pyproject.toml         # Package metadata
```

## Development

### Run Tests

```bash
pip install pytest pytest-asyncio
pytest tests/ -v
```

All tests should pass (75/75).

### Run Broker Locally (Development)

```bash
# With default config
python -m dp_mqtt

# With custom config
python -m dp_mqtt --config path/to/config.yaml

# With debug logging
python -m dp_mqtt --debug
```

## Docker Details

The Docker setup:

1. **Reads `config.yaml`** from project root
2. **Uses port from config** (`broker.port`)
3. **Mounts logs** to `./logs` directory
4. **Auto-restarts** on failure

The port is mapped as: `${MQTT_PORT:-1883}:1883`
- Reads `MQTT_PORT` environment variable if set
- Defaults to `1883` if not set
- Inside container always uses port from config.yaml

## Troubleshooting

### Port Already in Use

```bash
# Stop any existing MQTT brokers
docker stop dp-mqtt-broker

# Or check what's using the port
sudo lsof -i :1883
```

### Authentication Fails

1. Check credentials match `config.yaml`
2. Verify password hashes are correct
3. Check broker logs: `docker logs dp-mqtt-broker`

### Can't Connect

1. Ensure broker is running: `docker ps | grep mqtt`
2. Check firewall allows port 1883
3. Verify `broker.host` in config is `0.0.0.0` not `localhost`

### Config Not Loading

1. Ensure `config.yaml` exists in project root
2. Check YAML syntax is valid
3. Look for warnings in broker logs

## API Reference

### Client Class (dp_mqtt)

```python
from dp_mqtt import Client, MQTTMessage

# Create client
client = Client(client_id: str, clean_session: bool = True)

# Set authentication
client.username_pw_set(username: str, password: str)

# Set will message
client.will_set(topic: str, payload: str|bytes, qos: int = 0, retain: bool = False)

# Connect
await client.connect(host: str, port: int = 1883, keepalive: int = 60) -> int

# Subscribe
await client.subscribe(subscriptions: list[tuple[str, int]]) -> int

# Publish
await client.publish(topic: str, payload: str|bytes, qos: int = 0, retain: bool = False) -> int

# Disconnect
await client.disconnect()

# Callbacks
client.on_connect = lambda client, rc: ...
client.on_message = lambda client, msg: ...
client.on_subscribe = lambda client, mid, granted_qos: ...
client.on_publish = lambda client, mid: ...
client.on_disconnect = lambda client, error: ...
```

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `pytest tests/ -v`
5. Submit a pull request

## Support

For issues and questions:
- Open an issue on GitHub
- Check existing issues for solutions
- Review the troubleshooting section above
