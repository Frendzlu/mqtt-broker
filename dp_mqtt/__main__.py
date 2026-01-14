"""
MQTT Broker Entry Point
Run with: python -m dp_mqtt
"""

import argparse
import asyncio
import logging
import os
from datetime import datetime

from .broker import run_broker


def setup_logging(debug: bool = False, log_dir: str = "logs") -> str:
    """
    Configure logging to both console and timestamped log file.
    Returns the log file path.
    """
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"broker_{timestamp}.log")
    
    level = logging.DEBUG if debug else logging.INFO
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return log_file


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
        "--config", "-c",
        default=None,
        help="Path to config.yaml file (default: ./config.yaml if exists)"
    )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "--log-dir",
        default="logs",
        help="Directory for log files (default: logs)"
    )
    
    args = parser.parse_args()
    
    log_file = setup_logging(debug=args.debug, log_dir=args.log_dir)
    
    print(f"Starting MQTT Broker on {args.host}:{args.port}")
    if args.config:
        print(f"Config file: {args.config}")
    print(f"Logging to: {log_file}")
    print("Press Ctrl+C to stop")
    
    try:
        asyncio.run(run_broker(host=args.host, port=args.port, config_path=args.config))
    except KeyboardInterrupt:
        print("\nBroker stopped")


if __name__ == "__main__":
    main()
