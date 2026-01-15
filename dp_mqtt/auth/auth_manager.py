import hashlib
from pathlib import Path
from typing import Dict, Optional

import yaml

from dp_mqtt.auth.user import User
from dp_mqtt.broker.broker_config import BrokerConfig

import logging

logger = logging.getLogger(__name__)


class AuthManager:
    """Manages authentication and authorization for the broker."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize authentication manager.
        
        Args:
            config_path: Path to config.yaml file. If None, looks for config.yaml in current directory.
        """
        self.users: Dict[str, User] = {}
        self.allow_anonymous = True
        self.broker_config = BrokerConfig()
        
        if config_path is None:
            config_path = "config.yaml"
        
        self.config_path = Path(config_path)
        
        if self.config_path.exists():
            self._load_config()
        else:
            logger.warning(f"Config file not found: {config_path}. Running with default settings (anonymous allowed).")
    
    def _load_config(self) -> None:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config:
                logger.warning("Empty config file. Using default settings.")
                return
            
            # Load broker settings
            broker_settings = config.get('broker', {})
            self.broker_config = BrokerConfig(
                host=broker_settings.get('host', '0.0.0.0'),
                port=broker_settings.get('port', 1883),
                max_qos=broker_settings.get('max_qos', 2)
            )
            
            # Load authentication settings
            auth_config = config.get('authentication', {})
            self.allow_anonymous = auth_config.get('allow_anonymous', True)
            
            # Load users
            users_list = auth_config.get('users', [])
            for user_data in users_list:
                username = user_data.get('username')
                password = user_data.get('password')
                
                if not username or not password:
                    logger.warning(f"Invalid user entry in config: {user_data}")
                    continue
                
                # Hash the password if it's not already hashed
                if password.startswith('sha256:'):
                    password_hash = password[7:]  # Remove 'sha256:' prefix
                else:
                    # Plain text password - hash it
                    password_hash = self._hash_password(password)
                    logger.warning(f"Plain text password detected for user '{username}'. Consider using hashed passwords.")
                
                self.users[username] = User(
                    username=username,
                    password_hash=password_hash,
                    allowed_topics=user_data.get('allowed_topics')
                )
            
            logger.info(f"Loaded {len(self.users)} users from config. Anonymous: {self.allow_anonymous}")
        
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {e}")
        except Exception as e:
            logger.error(f"Error loading config: {e}")
    
    @staticmethod
    def _hash_password(password: str) -> str:
        """Hash a password using SHA256."""
        return hashlib.sha256(password.encode('utf-8')).hexdigest()
    
    def authenticate(self, username: Optional[str], password: Optional[bytes]) -> bool:
        """
        Authenticate a username/password combination.
        
        Args:
            username: Username or None for anonymous
            password: Password bytes or None for anonymous
        
        Returns:
            True if authentication succeeds, False otherwise
        """
        # Anonymous connection
        if username is None and password is None:
            return self.allow_anonymous
        
        # Missing username but has password, or vice versa
        if username is None or password is None:
            return False
        
        # Check if user exists
        user = self.users.get(username)
        if user is None:
            logger.warning(f"Authentication failed: unknown user '{username}'")
            return False
        
        # Verify password
        password_str = password.decode('utf-8', errors='ignore')
        password_hash = self._hash_password(password_str)
        
        if password_hash == user.password_hash:
            logger.info(f"User '{username}' authenticated successfully")
            return True
        else:
            logger.warning(f"Authentication failed: invalid password for user '{username}'")
            return False
    
    def is_authorized(self, username: Optional[str], topic: str, access_type: str = 'read') -> bool:
        """
        Check if user is authorized to access a topic.
        
        Args:
            username: Username or None for anonymous
            topic: Topic to check
            access_type: 'read' (subscribe) or 'write' (publish)
        
        Returns:
            True if authorized, False otherwise
        
        Note: Basic implementation - always returns True if authenticated.
              Future enhancement: implement topic-level ACL.
        """
        # For now, if authenticated, allow all topics
        # Future: check user.allowed_topics with pattern matching
        return True
    
    @staticmethod
    def generate_password_hash(password: str) -> str:
        """
        Generate a password hash for use in config file.
        
        Usage:
            from dp_mqtt.auth import AuthManager
            print(AuthManager.generate_password_hash("mypassword"))
        """
        return f"sha256:{hashlib.sha256(password.encode('utf-8')).hexdigest()}"