"""
Example: MQTT Subscriber Client

This example demonstrates how to use the MQTT client to subscribe and receive messages.
"""

import asyncio
from dp_mqtt import Client, MQTTMessage, setup_client_logging


async def main():
    log_file = setup_client_logging(debug=False, prefix="subscriber")
    print(f"Logging to: {log_file}")
    
    # Create client
    client = Client(client_id="example-subscriber")
    
    # Set up callbacks
    def on_connect(client: Client, rc: int):
        if rc == 0:
            print("Connected to broker!")
        else:
            print(f"Connection failed with code: {rc}")
    
    def on_message(client: Client, msg: MQTTMessage):
        print(f"Received message:")
        print(f"   Time:    {msg.timestamp_str}")
        print(f"   Topic:   {msg.topic}")
        print(f"   Payload: {msg.payload_str}")
        print(f"   QoS:     {msg.qos}")
        print(f"   Retain:  {msg.retain}")
        print()
    
    def on_subscribe(client: Client, mid: int, granted_qos: list):
        print(f"Subscribed (mid={mid}), granted QoS: {granted_qos}")
    
    def on_disconnect(client: Client, error):
        if error:
            print(f"Disconnected with error: {error}")
        else:
            print("Disconnected cleanly.")
    
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    
    try:
        print("Connecting to broker...")
        await client.connect("localhost", 1883, keepalive=60)
        
        # Subscribe to topics
        print("\nSubscribing to topics...")
        
        # Subscribe to specific topic
        await client.subscribe("sensors/temperature", qos=1)
        
        # Subscribe to wildcard topic
        await client.subscribe("sensors/#", qos=1)
        
        # Subscribe to multiple topics at once
        await client.subscribe([
            ("alerts/+", 2),
            ("status/online", 1),
        ])
        
        print("\nWaiting for messages... (Press Ctrl+C to stop)\n")
        
        # Run the event loop
        await client.loop_forever()
        
    except KeyboardInterrupt:
        print("\n\nStopping...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
