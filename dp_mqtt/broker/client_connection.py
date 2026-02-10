import asyncio
from dataclasses import dataclass
from typing import Optional

from dp_mqtt.session import WillMessage, Session


@dataclass
class ClientConnection:
    """Represents an active client connection."""
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    client_id: Optional[str] = None
    session: Optional[Session] = None
    connected: bool = False
    keep_alive: int = 0
    last_activity: float = 0
    will_message: Optional[WillMessage] = None
    clean_session: bool = True
    address: str = ""