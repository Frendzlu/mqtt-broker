"""
Example: MQTT Publisher Client

This example demonstrates how to use the MQTT client to publish messages.
"""

import asyncio
from dp_mqtt import Client, setup_client_logging


async def main():
    log_file = setup_client_logging(debug=False, prefix="publisher")
    print(f"Logging to: {log_file}")
    
    # Create client with unique ID
    client = Client(client_id="example-publisher")
    
    # Set up callbacks
    def on_connect(client: Client, rc: int):
        if rc == 0:
            print("Connected to broker!")
        else:
            print(f"Connection failed with code: {rc}")
    
    def on_publish(client: Client, mid: int):
        print(f"Message {mid} published")
    
    client.on_connect = on_connect
    client.on_publish = on_publish
    
    try:
        # Connect to broker
        print("Connecting to broker...")
        await client.connect("localhost", 1883, keepalive=60)
        
        # Publish some messages
        print("\nPublishing messages...")
        
        # QoS 0 - Fire and forget
        await client.publish("sensors/temperature", "22.5", qos=0)
        print("  Published temperature (QoS 0)")
        
        # QoS 1 - At least once
        await client.publish("sensors/humidity", "65", qos=1)
        print("  Published humidity (QoS 1)")
        
        # QoS 2 - Exactly once
        await client.publish("alerts/critical", "System alert!", qos=2)
        print("  Published alert (QoS 2)")
        
        # Retained message
        await client.publish("status/online", "true", qos=1, retain=True)
        print("  Published retained status")
        
        print("\nAll messages published successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.disconnect()
        print("Disconnected.")


if __name__ == "__main__":
    asyncio.run(main())
