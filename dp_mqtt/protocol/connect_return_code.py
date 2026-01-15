from enum import IntEnum


class ConnectReturnCode(IntEnum):
    """CONNACK Return Codes"""
    ACCEPTED = 0x00
    UNACCEPTABLE_PROTOCOL = 0x01
    IDENTIFIER_REJECTED = 0x02
    SERVER_UNAVAILABLE = 0x03
    BAD_USERNAME_PASSWORD = 0x04
    NOT_AUTHORIZED = 0x05