"""Đọc cấu hình môi trường dùng chung cho Tracker và Peer."""

from dataclasses import dataclass, field
import os

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str = field(repr=False)
    tracker_host: str
    tracker_port: int
    heartbeat_interval_seconds: int
    peer_ttl_seconds: int
    socket_timeout_seconds: int
    tracker_max_clients: int = 64
    tracker_cleanup_interval_seconds: int = 5


def load_settings() -> Settings:
    """Load .env once when a process starts; validate basic timeouts and ports."""
    load_dotenv()
    settings = Settings(
        postgres_host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "p2p_tracker"),
        postgres_user=os.getenv("POSTGRES_USER", "p2p_app"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", ""),
        tracker_host=os.getenv("TRACKER_HOST", "127.0.0.1"),
        tracker_port=int(os.getenv("TRACKER_PORT", "9000")),
        heartbeat_interval_seconds=int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "5")),
        peer_ttl_seconds=int(os.getenv("PEER_TTL_SECONDS", "20")),
        socket_timeout_seconds=int(os.getenv("SOCKET_TIMEOUT_SECONDS", "5")),
        tracker_max_clients=int(os.getenv("TRACKER_MAX_CLIENTS", "64")),
        tracker_cleanup_interval_seconds=int(os.getenv("TRACKER_CLEANUP_INTERVAL_SECONDS", "5")),
    )
    if not 1 <= settings.tracker_port <= 65535:
        raise ValueError("TRACKER_PORT must be between 1 and 65535")
    if not 1 <= settings.postgres_port <= 65535:
        raise ValueError("POSTGRES_PORT must be between 1 and 65535")
    if settings.tracker_max_clients <= 0 or settings.tracker_cleanup_interval_seconds <= 0:
        raise ValueError("Tracker client limit and cleanup interval must be positive")
    if settings.heartbeat_interval_seconds <= 0 or settings.peer_ttl_seconds <= 0:
        raise ValueError("Heartbeat interval and peer TTL must be positive")
    if settings.peer_ttl_seconds <= settings.heartbeat_interval_seconds:
        raise ValueError("PEER_TTL_SECONDS must be greater than HEARTBEAT_INTERVAL_SECONDS")
    if settings.socket_timeout_seconds <= 0:
        raise ValueError("SOCKET_TIMEOUT_SECONDS must be positive")
    return settings
