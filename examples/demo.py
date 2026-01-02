"""
Example: Complete MQTT Client Demo

This example demonstrates a full MQTT client workflow:
1. Connect with authentication and will message
2. Subscribe to topics
3. Publish messages
4. Receive messages
5. Disconnect gracefully
"""

import asyncio
from dp_mqtt import Client, MQTTMessage, setup_client_logging


async def main():
    log_file = setup_client_logging(debug=False, prefix="demo_client")
    print(f"Logging to: {log_file}")

    client = Client(client_id="demo-client", clean_session=True)
    
    # Optional: Set authentication
    # client.username_pw_set("user", "password")

    client.will_set(
        topic="clients/demo-client/status",
        payload="offline",
        qos=1,
        retain=True
    )
    
    message_count = 0
    
    def on_connect(client: Client, rc: int):
        codes = {
            0: "Connection accepted",
            1: "Unacceptable protocol version",
            2: "Identifier rejected",
            3: "Server unavailable",
            4: "Bad username or password",
            5: "Not authorized",
        }
        print(f"[CONNECT] {codes.get(rc, f'Unknown code {rc}')}")
    
    def on_message(client: Client, msg: MQTTMessage):
        nonlocal message_count
        message_count += 1
        print(f"[MESSAGE #{message_count}] [{msg.timestamp_str}] {msg.topic}: {msg.payload_str}")
    
    def on_subscribe(client: Client, mid: int, granted_qos: list):
        print(f"[SUBSCRIBE] Confirmed (mid={mid}), QoS granted: {granted_qos}")
    
    def on_publish(client: Client, mid: int):
        print(f"[PUBLISH] Message {mid} delivered")
    
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_publish = on_publish
    
    try:
        # Connect
        print("=" * 50)
        print("MQTT Client Demo")
        print("=" * 50)
        
        rc = await client.connect("localhost", 1883, keepalive=30)
        if rc != 0:
            return
        
        # Publish online status
        await client.publish("clients/demo-client/status", "online", qos=1, retain=True)
        
        # Subscribe to topics
        print("\n--- Subscribing ---")
        await client.subscribe([
            ("demo/messages", 1),
            ("demo/commands/#", 2),
        ])
        
        # Start background loop
        await client.loop_start()
        
        # Publish some test messages
        print("\n--- Publishing test messages ---")
        for i in range(3):
            await client.publish("demo/messages", f"Test message {i+1}", qos=1)
            await asyncio.sleep(0.5)
        
        # Wait and receive messages
        print("\n--- Waiting for messages (5 seconds) ---")
        await asyncio.sleep(5)
        
        # Unsubscribe
        print("\n--- Unsubscribing ---")
        await client.unsubscribe("demo/commands/#")
        
        # Final stats
        print("\n--- Summary ---")
        print(f"Total messages received: {message_count}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Graceful disconnect
        await client.loop_stop()
        await client.disconnect()
        print("\nDemo complete!")


if __name__ == "__main__":
    asyncio.run(main())
