#!/usr/bin/env python3
"""
Simple MQTT Client Example using paho-mqtt

This example demonstrates connecting to the dp-mqtt broker
using the popular paho-mqtt library.

Install paho-mqtt first:
    pip install paho-mqtt
"""

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import time


# Configuration
BROKER_HOST = "localhost"
BROKER_PORT = 1883
USERNAME = "admin"
PASSWORD = "admin"

# State
message_count = 0


def on_connect(client, userdata, flags, reason_code, properties=None):
    """Called when connected to broker"""
    rc = reason_code.value if hasattr(reason_code, 'value') else int(reason_code)
    if rc == 0:
        print("Connected to broker")
        # Subscribe to topics after connecting
        client.subscribe("test/#", qos=1)
        client.subscribe("sensor/+/data", qos=0)
        print("Subscribed to topics")
    else:
        print(f"Connection failed with code: {rc}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    """Called when disconnected from broker"""
    print("Disconnected from broker")


def on_message(client, userdata, msg):
    """Called when a message is received"""
    global message_count
    message_count += 1
    payload = msg.payload.decode('utf-8', errors='replace')
    retain_str = " [RETAINED]" if msg.retain else ""
    print(f"Message #{message_count}: [{msg.topic}] {payload} (QoS {msg.qos}){retain_str}")


def on_publish(client, userdata, mid, reason_code=None, properties=None):
    """Called when a message is published"""
    print(f"Message {mid} published")


def on_subscribe(client, userdata, mid, reason_codes, properties=None):
    """Called when subscribed to topic"""
    pass  # Already printed in on_connect


def main():
    print("MQTT Client Example using paho-mqtt")
    print("=" * 50)
    
    # Create client with MQTT v3.1.1
    client = mqtt.Client(
        client_id="paho-demo-client",
        protocol=mqtt.MQTTv311,
        callback_api_version=CallbackAPIVersion.VERSION2
    )
    
    # Set authentication
    client.username_pw_set(USERNAME, PASSWORD)
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_subscribe = on_subscribe
    
    try:
        # Connect to broker
        print(f"\nConnecting to {BROKER_HOST}:{BROKER_PORT}...")
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        
        # Start network loop in background
        client.loop_start()
        
        # Wait for connection
        time.sleep(1)
        
        # Publish some messages
        print("\nPublishing messages...")
        client.publish("test/message", "Hello from paho-mqtt!", qos=1)
        client.publish("sensor/temp/data", "23.0", qos=0)
        client.publish("sensor/humidity/data", "68", qos=1)
        
        # Wait for messages
        print("\nListening for messages (5 seconds)...")
        time.sleep(5)
        
        # Disconnect
        print("\nDisconnecting...")
        client.loop_stop()
        client.disconnect()
        
        # Wait for clean disconnect
        time.sleep(1)
        print("Complete")
        
    except Exception as e:
        print(f"Error: {e}")
        client.loop_stop()


if __name__ == "__main__":
    main()
