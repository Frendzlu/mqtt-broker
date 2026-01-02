"""
Tests for Topic management.
"""

import pytest
from dp_mqtt.topics import (
    TopicManager, RetainedMessage,
    validate_topic_name, validate_topic_filter, topic_matches_filter
)


class TestTopicManager:
    """Tests for TopicManager class."""
    
    def test_set_retained_message(self):
        manager = TopicManager()
        manager.set_retained_message("test/topic", b"hello", 1)
        
        msg = manager.get_retained_message("test/topic")
        assert msg is not None
        assert msg.payload == b"hello"
        assert msg.qos == 1
    
    def test_empty_payload_clears_retained(self):
        manager = TopicManager()
        manager.set_retained_message("test/topic", b"hello", 1)
        manager.set_retained_message("test/topic", b"", 0)
        
        msg = manager.get_retained_message("test/topic")
        assert msg is None
    
    def test_get_nonexistent_retained(self):
        manager = TopicManager()
        msg = manager.get_retained_message("nonexistent")
        assert msg is None
    
    def test_get_matching_retained_exact(self):
        manager = TopicManager()
        manager.set_retained_message("sport/tennis", b"tennis data", 0)
        manager.set_retained_message("sport/football", b"football data", 0)
        
        matches = manager.get_matching_retained_messages("sport/tennis", topic_matches_filter)
        assert len(matches) == 1
        assert matches[0].payload == b"tennis data"
    
    def test_get_matching_retained_wildcard(self):
        manager = TopicManager()
        manager.set_retained_message("sport/tennis", b"tennis", 0)
        manager.set_retained_message("sport/football", b"football", 0)
        manager.set_retained_message("finance/stock", b"stock", 0)
        
        matches = manager.get_matching_retained_messages("sport/#", topic_matches_filter)
        assert len(matches) == 2
        
        topics = {m.topic for m in matches}
        assert "sport/tennis" in topics
        assert "sport/football" in topics
    
    def test_clear_retained_message(self):
        manager = TopicManager()
        manager.set_retained_message("test/topic", b"hello", 1)
        manager.clear_retained_message("test/topic")
        
        msg = manager.get_retained_message("test/topic")
        assert msg is None


class TestTopicNameValidation:
    """Tests for topic name validation."""
    
    def test_valid_topic_names(self):
        assert validate_topic_name("sport/tennis") is True
        assert validate_topic_name("sport") is True
        assert validate_topic_name("/leading/slash") is True
        assert validate_topic_name("trailing/slash/") is True
        assert validate_topic_name("//double//slashes") is True
    
    def test_invalid_topic_names(self):
        assert validate_topic_name("") is False
        assert validate_topic_name("sport/+/player") is False  # No wildcards
        assert validate_topic_name("sport/#") is False  # No wildcards


class TestTopicFilterValidation:
    """Tests for topic filter validation."""
    
    def test_valid_filters(self):
        assert validate_topic_filter("sport/tennis") is True
        assert validate_topic_filter("+") is True
        assert validate_topic_filter("#") is True
        assert validate_topic_filter("sport/+") is True
        assert validate_topic_filter("sport/#") is True
        assert validate_topic_filter("+/tennis") is True
        assert validate_topic_filter("sport/+/player1") is True
    
    def test_invalid_filters(self):
        assert validate_topic_filter("") is False
        assert validate_topic_filter("sport/ten+is") is False  # + not alone
        assert validate_topic_filter("sport/#/more") is False  # # not at end
        assert validate_topic_filter("sport/te#") is False  # # not alone


class TestTopicMatching:
    """Comprehensive tests for topic matching."""
    
    def test_exact_match(self):
        assert topic_matches_filter("sport/tennis/player1", "sport/tennis/player1") is True
        assert topic_matches_filter("sport/tennis/player1", "sport/tennis/player2") is False
    
    def test_single_level_wildcard(self):
        # + matches any single level
        assert topic_matches_filter("sport/tennis/player1", "sport/+/player1") is True
        assert topic_matches_filter("sport/football/player1", "sport/+/player1") is True
        assert topic_matches_filter("sport/tennis/player2", "sport/+/player1") is False
        
        # + can be at any position
        assert topic_matches_filter("sport/tennis", "+/tennis") is True
        assert topic_matches_filter("sport/tennis", "sport/+") is True
        
        # + matches exactly one level
        assert topic_matches_filter("sport/tennis/player1", "sport/+") is False
        assert topic_matches_filter("sport", "sport/+") is False
    
    def test_multi_level_wildcard(self):
        # # matches zero or more levels
        assert topic_matches_filter("sport", "sport/#") is True
        assert topic_matches_filter("sport/tennis", "sport/#") is True
        assert topic_matches_filter("sport/tennis/player1", "sport/#") is True
        assert topic_matches_filter("sport/tennis/player1/stats", "sport/#") is True
        
        # # alone matches everything
        assert topic_matches_filter("anything", "#") is True
        assert topic_matches_filter("anything/at/all", "#") is True
    
    def test_combined_wildcards(self):
        assert topic_matches_filter("sport/tennis/player1", "sport/+/#") is True
        assert topic_matches_filter("sport/tennis/player1/stats", "sport/+/#") is True
        assert topic_matches_filter("sport/tennis", "sport/+/#") is True
    
    def test_empty_levels(self):
        # Double slash creates empty level
        assert topic_matches_filter("sport//tennis", "sport//tennis") is True
        assert topic_matches_filter("sport//tennis", "sport/+/tennis") is True
    
    def test_system_topics(self):
        # $ topics should not match leading wildcards
        assert topic_matches_filter("$SYS/broker/clients", "#") is False
        assert topic_matches_filter("$SYS/broker/clients", "+/broker/clients") is False
        
        # But explicit $ filter works
        assert topic_matches_filter("$SYS/broker/clients", "$SYS/#") is True
        assert topic_matches_filter("$SYS/broker/clients", "$SYS/broker/clients") is True
        
        # Regular topics still match #
        assert topic_matches_filter("sys/broker", "#") is True
    
    def test_edge_cases(self):
        # Leading/trailing slashes
        assert topic_matches_filter("/sport/tennis", "/sport/tennis") is True
        assert topic_matches_filter("/sport/tennis", "+/sport/tennis") is True
        assert topic_matches_filter("sport/tennis/", "sport/tennis/") is True
        
        # Single level topic
        assert topic_matches_filter("sport", "sport") is True
        assert topic_matches_filter("sport", "+") is True
        assert topic_matches_filter("sport", "#") is True
