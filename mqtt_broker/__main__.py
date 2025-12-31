"""
MQTT Broker Entry Point
Run with: python -m mqtt_broker
"""

import argparse
import asyncio
import logging

from .broker import run_broker


def main():
    parser = argparse.ArgumentParser(description="MQTT 3.1.1 Broker")
    parser.add_argument(
        "--host", "-H",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=1883,
        help="Port to bind to (default: 1883)"
    )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Enable debug logging"
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    print(f"Starting MQTT Broker on {args.host}:{args.port}")
    print("Press Ctrl+C to stop")
    
    try:
        asyncio.run(run_broker(host=args.host, port=args.port))
    except KeyboardInterrupt:
        print("\nBroker stopped")


if __name__ == "__main__":
    main()
