from abc import ABC


class TopicUtils(ABC):
    @staticmethod
    def validate_topic_name(topic: str) -> bool:
        """
        Validate a topic name (for PUBLISH).
        Topic names cannot contain wildcards.
        """
        if not topic:
            return False
        if '#' in topic or '+' in topic:
            return False
        # Check for valid UTF-8 is done at string parsing level
        return True

    @staticmethod
    def validate_topic_filter(topic_filter: str) -> bool:
        """
        Validate a topic filter (for SUBSCRIBE).
        + can only occupy an entire level.
        # can only be at the end and must occupy entire level.
        """
        if not topic_filter:
            return False
        
        levels = topic_filter.split('/')
        
        for i, level in enumerate(levels):
            if '#' in level:
                # # must be alone and at the end
                if level != '#' or i != len(levels) - 1:
                    return False
            elif '+' in level:
                # + must be alone in its level
                if level != '+':
                    return False
        
        return True

    @staticmethod
    def topic_matches_filter(topic: str, filter_pattern: str) -> bool:
        """
        Check if a topic name matches a topic filter pattern.
        Supports + (single level) and # (multi-level) wildcards.
        
        Topics starting with $ are system topics and should not match 
        filters starting with # or + at the first level.
        """
        # System topics special handling
        if topic.startswith('$') and filter_pattern.startswith(('+', '#')):
            return False
        
        topic_levels = topic.split('/')
        filter_levels = filter_pattern.split('/')
        
        topic_idx = 0
        filter_idx = 0
        
        while filter_idx < len(filter_levels):
            filter_level = filter_levels[filter_idx]
            
            if filter_level == '#':
                # Multi-level wildcard matches everything from here
                return True
            
            if topic_idx >= len(topic_levels):
                # Topic is shorter than filter
                return False
            
            topic_level = topic_levels[topic_idx]
            
            if filter_level == '+':
                # Single-level wildcard matches any single level
                pass
            elif filter_level != topic_level:
                # Levels don't match
                return False
            
            topic_idx += 1
            filter_idx += 1
        
        # Both must be exhausted for a match
        return topic_idx == len(topic_levels)