"""
Tests for Session management.
"""

import pytest
from dp_mqtt.session import Session, SessionManager, WillMessage, PendingMessage


class TestSession:
    """Tests for Session class."""
    
    def test_create_session(self):
        session = Session(client_id="test_client")
        assert session.client_id == "test_client"
        assert len(session.subscriptions) == 0
        assert len(session.pending_outgoing) == 0
    
    def test_add_subscription(self):
        session = Session(client_id="test")
        session.add_subscription("topic/test", 1)
        assert session.subscriptions["topic/test"] == 1
    
    def test_update_subscription(self):
        session = Session(client_id="test")
        session.add_subscription("topic/test", 1)
        session.add_subscription("topic/test", 2)
        assert session.subscriptions["topic/test"] == 2
    
    def test_remove_subscription(self):
        session = Session(client_id="test")
        session.add_subscription("topic/test", 1)
        result = session.remove_subscription("topic/test")
        assert result is True
        assert "topic/test" not in session.subscriptions
    
    def test_remove_nonexistent_subscription(self):
        session = Session(client_id="test")
        result = session.remove_subscription("nonexistent")
        assert result is False
    
    def test_get_next_packet_id(self):
        session = Session(client_id="test")
        id1 = session.get_next_packet_id()
        id2 = session.get_next_packet_id()
        assert id1 == 1
        assert id2 == 2
    
    def test_packet_id_wraps(self):
        session = Session(client_id="test")
        session.next_packet_id = 65535
        id1 = session.get_next_packet_id()
        id2 = session.get_next_packet_id()
        assert id1 == 65535
        assert id2 == 1
    
    def test_packet_id_skips_in_use(self):
        session = Session(client_id="test")
        session.pending_outgoing[1] = PendingMessage(
            packet_id=1, topic="test", payload=b"", qos=1, retain=False, timestamp=0
        )
        id1 = session.get_next_packet_id()
        assert id1 == 2  # Skipped 1 because it's in use
    
    def test_clear_session(self):
        session = Session(client_id="test")
        session.add_subscription("topic", 1)
        session.pending_outgoing[1] = PendingMessage(
            packet_id=1, topic="test", payload=b"", qos=1, retain=False, timestamp=0
        )
        session.pending_incoming_qos2.add(2)
        
        session.clear()
        
        assert len(session.subscriptions) == 0
        assert len(session.pending_outgoing) == 0
        assert len(session.pending_incoming_qos2) == 0
        assert session.next_packet_id == 1


class TestSessionManager:
    """Tests for SessionManager class."""
    
    def test_create_new_session(self):
        manager = SessionManager()
        session, present = manager.get_or_create_session("client1", clean_session=True)
        
        assert session.client_id == "client1"
        assert present is False
    
    def test_get_existing_session(self):
        manager = SessionManager()
        session1, _ = manager.get_or_create_session("client1", clean_session=False)
        session1.add_subscription("topic", 1)
        
        session2, present = manager.get_or_create_session("client1", clean_session=False)
        
        assert session2 is session1
        assert present is True
        assert "topic" in session2.subscriptions
    
    def test_clean_session_clears_existing(self):
        manager = SessionManager()
        session1, _ = manager.get_or_create_session("client1", clean_session=False)
        session1.add_subscription("topic", 1)
        
        session2, present = manager.get_or_create_session("client1", clean_session=True)
        
        assert session2 is session1  # Same object
        assert present is False  # But session not considered present
        assert len(session2.subscriptions) == 0  # Cleared
    
    def test_remove_session(self):
        manager = SessionManager()
        manager.get_or_create_session("client1", clean_session=False)
        
        assert manager.has_session("client1") is True
        
        manager.remove_session("client1")
        
        assert manager.has_session("client1") is False
    
    def test_remove_nonexistent_session(self):
        manager = SessionManager()
        # Should not raise
        manager.remove_session("nonexistent")


class TestWillMessage:
    """Tests for WillMessage class."""
    
    def test_create_will_message(self):
        will = WillMessage(
            topic="client/status",
            payload=b"offline",
            qos=1,
            retain=True
        )
        
        assert will.topic == "client/status"
        assert will.payload == b"offline"
        assert will.qos == 1
        assert will.retain is True
