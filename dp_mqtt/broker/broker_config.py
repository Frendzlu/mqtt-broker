from dataclasses import dataclass


@dataclass
class BrokerConfig:
    """Broker configuration settings."""
    host: str = "0.0.0.0"
    port: int = 1883
    max_qos: int = 2