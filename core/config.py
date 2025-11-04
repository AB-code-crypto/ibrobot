from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class IBConfig:
    host: str
    port: int
    client_id: int
    base_retry_delay: float
    max_retry_delay: float
    health_check_period: float


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    fmt: str


@dataclass(frozen=True)
class DBConfig:
    sqlite_path: str
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    temp_store: str = "MEMORY"
    busy_timeout_ms: int = 5000


@dataclass(frozen=True)
class TelegramConfig:
    enabled: bool
    bot_token: str
    chat_id_alerts: int
    chat_id_logs: int


@dataclass(frozen=True)
class HistoryConfig:
    timeframe_sec: int = 5  # базовая частота
    use_rth: int = 0  # 0 = расширенный RTH (как обсудили)
    what_to_show: str = "TRADES"  # источник
