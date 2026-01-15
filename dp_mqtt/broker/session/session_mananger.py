from typing import Dict, Tuple
from .session import Session


class SessionManager:
    """Manages all client sessions."""
    
    def __init__(self):
        self.sessions: Dict[str, Session] = {}
    
    def get_or_create_session(self, client_id: str, clean_session: bool) -> Tuple[Session, bool]:
        """
        Get existing session or create new one.
        Returns (session, session_present).
        If clean_session is True, any existing session is cleared.
        """
        session_present = False
        
        if client_id in self.sessions:
            session = self.sessions[client_id]
            if clean_session:
                session.clear()
            else:
                session_present = True
        else:
            session = Session(client_id=client_id)
            self.sessions[client_id] = session
        
        return session, session_present
    
    def remove_session(self, client_id: str) -> None:
        """Remove a session completely."""
        if client_id in self.sessions:
            del self.sessions[client_id]
    
    def has_session(self, client_id: str) -> bool:
        """Check if a session exists."""
        return client_id in self.sessions