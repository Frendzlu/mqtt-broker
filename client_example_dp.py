#!/usr/bin/env python3
"""
Simple MQTT Client Example using dp_mqtt

This example demonstrates:
- Connecting with authentication
- Publishing messages
- Subscribing to topics
- Receiving messages
"""

import asyncio
from dp_mqtt import Client, MQTTMessage


async def main():
    # Create client
    client_pub = Client(client_id="dp-mqtt-demo-client-pub", clean_session=True)
    client_sub = Client(client_id="dp-mqtt-demo-client-sub", clean_session=True)
    # Set authentication (must match credentials in config.yaml)
    client_pub.username_pw_set("admin", "admin")
    client_sub.username_pw_set("admin", "admin")
    
    # Message counter
    message_count = [0]
    
    # Callbacks
    def on_connect(client: Client, rc: int):
        if rc == 0:
            print("Connected to broker")
        else:
            print(f"Connection failed with code: {rc}")
    
    def on_message(client: Client, msg: MQTTMessage):
        message_count[0] += 1
        print(f"Message #{message_count[0]}: [{msg.topic}] {msg.payload_str}")
    
    def on_subscribe(client: Client, mid: int, granted_qos: list):
        print(f"Subscribed (QoS: {granted_qos})")
    
    def on_publish(client: Client, mid: int):
        print(f"Message {mid} published")
    
    # Assign callbacks
    client_pub.on_connect = on_connect
    client_pub.on_message = on_message
    client_pub.on_subscribe = on_subscribe
    client_pub.on_publish = on_publish
    client_sub.on_connect = on_connect
    client_sub.on_message = on_message
    client_sub.on_subscribe = on_subscribe
    client_sub.on_publish = on_publish
    
    try:
        # Connect to broker
        print("Connecting to MQTT broker...")
        rc = await client_pub.connect("localhost", 1883, keepalive=60)
        rc_sub = await client_sub.connect("localhost", 1883, keepalive=60)
        
        if rc != 0 or rc_sub != 0:
            print(f"Failed to connect. Error code: {rc}, {rc_sub}")
            return
        
        # Start network loops to receive messages
        await client_pub.loop_start()
        await client_sub.loop_start()
        
        # Subscribe to topics
        print("\nSubscribing to topics...")
        await client_sub.subscribe([
            ("test/#", 1),
            ("sensor/+/data", 0),
        ])
        
        # Publish some messages
        print("\nPublishing messages...")
        await client_pub.publish("test/message", "Hello from dp_mqtt!", qos=1)
        await client_pub.publish("sensor/temp/data", "22.5", qos=0)
        await client_pub.publish("sensor/humidity/data", "65", qos=1)

        # Wait for messages
        print("\nListening for messages...")
        await asyncio.sleep(5)
        
        # Stop network loops
        await client_pub.loop_stop()
        await client_sub.loop_stop()
        
        # Disconnect
        print("\nDisconnecting...")
        await client_pub.disconnect()
        await client_sub.disconnect()
        print("Disconnected")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
