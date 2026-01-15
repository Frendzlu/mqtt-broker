from dataclasses import dataclass


@dataclass
class ConnectFlags:
    """CONNECT packet flags"""
    clean_session: bool = False
    will_flag: bool = False
    will_qos: int = 0
    will_retain: bool = False
    password_flag: bool = False
    username_flag: bool = False