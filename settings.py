from __future__ import annotations
from core.config import IBConfig, LoggingConfig, DBConfig, TelegramConfig, HistoryConfig

# ==== IB / соединение ====
IB_CONFIG = IBConfig(
    host="127.0.0.1",
    port=7496,
    client_id=101,
    base_retry_delay=1.5,  # сек
    max_retry_delay=30.0,  # сек
    health_check_period=2.0  # сек
)

# ==== Логирование ====
LOGGING = LoggingConfig(
    level="DEBUG",
    fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)

# ==== База данных (на будущее шаг 2) ====
DATABASE = DBConfig(
    sqlite_path="./data/ibrobot.sqlite3",
    journal_mode="WAL",
    synchronous="NORMAL",
    temp_store="MEMORY",
    busy_timeout_ms=5000,
)

# ==== Телеграм (пока не используем, но храним тут) ====
TELEGRAM = TelegramConfig(
    enabled=False,
    bot_token="",  # добавишь позже
    chat_id_alerts=0,  # добавишь позже
    chat_id_logs=0,  # добавишь позже
)

# ==== История/агрегация ====
HISTORY = HistoryConfig(
    timeframe_sec=5,
    use_rth=0,
    what_to_show="TRADES",
)
