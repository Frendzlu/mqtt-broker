"""
MQTT Client Package
"""

import logging
import os
from datetime import datetime

from .client_impl import Client
from .message import MQTTMessage
from .errors import MQTTError, ConnectionError
from .config import MQTTClientConfig


logger = logging.getLogger(__name__)


def setup_client_logging(
    debug: bool = False,
    log_dir: str = "logs",
    prefix: str = "client"
) -> str:
    """
    Configure logging to both console and timestamped log file for clients.
    
    Args:
        debug: Enable debug-level logging
        log_dir: Directory for log files
        prefix: Prefix for log file name
        
    Returns:
        Path to the log file
    """
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{prefix}_{timestamp}.log")
    
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


__all__ = [
    "Client",
    "MQTTMessage",
    "MQTTError",
    "ConnectionError",
    "MQTTClientConfig",
    "setup_client_logging",
]
