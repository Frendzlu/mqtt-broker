from dataclasses import dataclass
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


@dataclass
class BrokerConfig:
    """Broker configuration settings."""
    host: str = "0.0.0.0"
    port: int = 1883
    max_qos: int = 2

    @classmethod
    def from_config_file(cls, config_path: Path) -> 'BrokerConfig':
        
        if not config_path.exists():
            logger.warning(f"Config file not found: {config_path}. Running with default settings (anonymous allowed).")
            return cls()
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config:
                logger.warning("Empty config file. Using default settings.")
                return cls()
            
            broker_settings = config.get('broker', {})
            return cls(
                host=broker_settings.get('host', '0.0.0.0'),
                port=broker_settings.get('port', 1883),
                max_qos=broker_settings.get('max_qos', 2)
            )

        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {e}. Using default settings.")
        except Exception as e:
            logger.error(f"Error loading config: {e}. Using default settings.")
        return cls()