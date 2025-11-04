from __future__ import annotations
from dataclasses import dataclass

# =========================
# Типы со встроенными дефолтами
# =========================

@dataclass(frozen=True)
class IBConfig:
    host: str = "127.0.0.1"
    port: int = 7496
    client_id: int = 101
    base_retry_delay: float = 1.5      # сек
    max_retry_delay: float = 30.0      # сек
    health_check_period: float = 2.0   # сек


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "DEBUG"
    fmt: str = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


@dataclass(frozen=True)
class DBConfig:
    sqlite_path: str = "./data/ibrobot.sqlite3"
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    temp_store: str = "MEMORY"
    busy_timeout_ms: int = 5000


@dataclass(frozen=True)
class TelegramConfig:
    # единый бот, два канала: торговля (приказы) и логи
    enabled_trade: bool = False
    enabled_logs: bool = True
    bot_token: str = "8121278489:AAFrj5FlOQmT4lctIfHOFmkqOqDL60vq5zg"
    chat_id_trade: int = -1002621383506
    chat_id_logs: int = -1003208160378


@dataclass(frozen=True)
class HistoryConfig:
    timeframe_sec: int = 5        # базовая частота (5s)
    use_rth: int = 0              # 0 = расширенный RTH
    what_to_show: str = "TRADES"  # источник данных


# =========================
# Экземпляры конфигов (удобно импортировать)
# =========================

IB_CONFIG = IBConfig()
LOGGING = LoggingConfig()
DATABASE = DBConfig()
TELEGRAM = TelegramConfig()
HISTORY = HistoryConfig()

__all__ = [
    "IBConfig", "LoggingConfig", "DBConfig", "TelegramConfig", "HistoryConfig",
    "IB_CONFIG", "LOGGING", "DATABASE", "TELEGRAM", "HISTORY",
]
