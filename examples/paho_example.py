"""
Example: Using paho-mqtt with the dp-mqtt broker

This example demonstrates how to use the popular paho-mqtt library
to connect to the dp-mqtt broker.

Usage:
    First, start the broker in one terminal:
        python -m dp_mqtt
    
    Then run this example in another terminal:
        python examples/paho_example.py
"""

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import time

# Configuration
BROKER_HOST = "localhost"
BROKER_PORT = 1883

# Track state
connected = False
messages_received = []


def on_connect(client, userdata, flags, reason_code, properties=None):
    global connected
    rc = reason_code.value if hasattr(reason_code, 'value') else int(reason_code)
    if rc == 0:
        print(f"Connected to broker at {BROKER_HOST}:{BROKER_PORT}")
        connected = True
    else:
        print(f"Connection failed with code: {rc}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    global connected
    connected = False
    print("Disconnected from broker")


def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8', errors='replace')
    messages_received.append((msg.topic, payload, msg.qos, msg.retain))
    retain_str = " [RETAINED]" if msg.retain else ""
    print(f"  Received: {msg.topic} = '{payload}' (QoS {msg.qos}){retain_str}")


def on_publish(client, userdata, mid, reason_code=None, properties=None):
    print(f"  Published message {mid}")


def on_subscribe(client, userdata, mid, reason_codes, properties=None):
    print(f"  Subscribed (mid={mid})")


def main():
    global connected, messages_received
    
    print("=" * 60)
    print("PAHO-MQTT Example for dp-mqtt Broker")
    print("=" * 60)
    print()
    
    # Create client
    client = mqtt.Client(
        client_id="paho-example-client",
        protocol=mqtt.MQTTv311,
        callback_api_version=CallbackAPIVersion.VERSION2
    )
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_subscribe = on_subscribe
    
    # Set a will message
    client.will_set("clients/paho-example/status", "offline", qos=1, retain=True)
    
    try:
        # === CONNECT ===
        print("[1] Connecting to broker...")
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        client.loop_start()
        
        # Wait for connection
        for _ in range(30):
            if connected:
                break
            time.sleep(0.1)
        
        if not connected:
            print("Failed to connect to broker!")
            print("  Make sure the broker is running: python -m dp_mqtt")
            return 1
        
        # Publish online status
        client.publish("clients/paho-example/status", "online", qos=1, retain=True)
        time.sleep(0.5)
        
        # === SUBSCRIBE ===
        print("\n[2] Subscribing to topics...")
        client.subscribe("test/messages", qos=1)
        client.subscribe("sensors/#", qos=1)  # Wildcard
        time.sleep(0.5)
        
        # === PUBLISH QoS 0 ===
        print("\n[3] Publishing with QoS 0 (fire and forget)...")
        client.publish("test/messages", "Hello QoS 0!", qos=0)
        time.sleep(0.5)
        
        # === PUBLISH QoS 1 ===
        print("\n[4] Publishing with QoS 1 (at least once)...")
        client.publish("test/messages", "Hello QoS 1!", qos=1)
        time.sleep(0.5)
        
        # === PUBLISH QoS 2 ===
        print("\n[5] Publishing with QoS 2 (exactly once)...")
        client.publish("test/messages", "Hello QoS 2!", qos=2)
        time.sleep(1)
        
        # === WILDCARD TEST ===
        print("\n[6] Testing wildcard subscription (sensors/#)...")
        client.publish("sensors/temperature", "22.5", qos=0)
        client.publish("sensors/humidity", "65", qos=0)
        client.publish("sensors/room1/temperature", "23.0", qos=0)
        time.sleep(1)
        
        # === CHECK MESSAGES ===
        print(f"\n[7] Messages received so far: {len(messages_received)}")
        
        # === RETAINED MESSAGE ===
        print("\n[8] Testing retained messages...")
        client.publish("test/retained", "This message is retained", qos=1, retain=True)
        time.sleep(0.5)
        
        # Subscribe to get the retained message
        messages_received.clear()
        client.subscribe("test/retained", qos=1)
        time.sleep(1)
        
        retained_found = any(r for t, p, q, r in messages_received if r)
        if retained_found:
            print("  Retained message received!")
        else:
            print("  (Retained message not received yet)")
        
        # Clear the retained message
        client.publish("test/retained", "", qos=1, retain=True)
        
        # === UNSUBSCRIBE ===
        print("\n[9] Unsubscribing from test/messages...")
        client.unsubscribe("test/messages")
        time.sleep(0.5)
        
        # === SUMMARY ===
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total messages received: {len(messages_received)}")
        print("All operations completed successfully!")
        
        # === DISCONNECT ===
        print("\n[10] Disconnecting...")
        client.publish("clients/paho-example/status", "", qos=1, retain=True)  # Clear status
        time.sleep(0.3)
        client.disconnect()
        client.loop_stop()
        
        print("\nDone!")
        return 0
        
    except ConnectionRefusedError:
        print("Connection refused!")
        print("  Make sure the broker is running: python -m dp_mqtt")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
