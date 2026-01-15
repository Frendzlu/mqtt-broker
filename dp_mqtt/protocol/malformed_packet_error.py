from .protocol_error import ProtocolError


class MalformedPacketError(ProtocolError):
    """Raised when packet structure is invalid"""
    pass