"""
MQTT Client Errors
"""


class MQTTError(Exception):
    """Base MQTT client error."""
    pass


class ConnectionError(MQTTError):
    """Connection-related error."""
    pass
