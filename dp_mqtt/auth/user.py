from dataclasses import dataclass
from typing import List, Optional


@dataclass
class User:
    """Represents an authenticated user."""
    username: str
    password_hash: str
    allowed_topics: Optional[List[str]] = None  # Future: topic-level ACL