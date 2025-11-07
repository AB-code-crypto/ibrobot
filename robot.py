# ibrobot/robot.py
from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Optional

from core.config import LOGGING, IB_CONFIG, TELEGRAM
from core.ib_connection import IBConnectionService
from core.portfolio_watch import PortfolioWatcher
from core.telegram import TelegramClient
from core.bars_collector import BarsCollector, BarsCollectorConfig

# ---- Константы запуска (минимально нужное) ----------------------------------

# Рабочий инструмент – активный фьючерс (можно поменять одной строкой)
ACTIVE_LOCAL_SYMBOL: str = "MNQZ5"

DB_PATH: Path = Path(__file__).parent / "data" / "ib_bars.sqlite"


# ---- Вспомогательное ---------------------------------------------------------

def _level_to_int(name: str) -> int:
    name_u = (name or "").upper()
    if name_u == "DEBUG":
        return logging.DEBUG
    if name_u == "INFO":
        return logging.INFO
    if name_u == "WARNING":
        return logging.WARNING
    if name_u == "ERROR":
        return logging.ERROR
    if name_u == "CRITICAL":
        return logging.CRITICAL
    return logging.INFO


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=_level_to_int(LOGGING.level),
        format=str(LOGGING.fmt),
    )
    return logging.getLogger("robot")


# ---- Основной запускатор -----------------------------------------------------

async def run_all(stop_event: asyncio.Event) -> None:
    log = setup_logging()
    log.info("🚀 Робот стартует. Лог-уровень: %s", LOGGING.level)

    # 1) Телеграм (используем конфиг напрямую, без дублей)
    tg = TelegramClient(TELEGRAM.bot_token)

    # 2) Сервис соединения с IB
    ib_svc = IBConnectionService(IB_CONFIG, log)

    # 3) Вотчер портфеля (события открытия/закрытия/частичные изменения уже реализованы в core)
    watcher = PortfolioWatcher(
        ib=ib_svc.ib,
        tg=tg,
        chat_id_logs=TELEGRAM.chat_id_logs,
        poll_snapshot_on_connect=True,
    )

    # 4) Сборщик 5-сек баров в SQLite (активный + соседние фьючерсы)
    bars_cfg = BarsCollectorConfig(
        db_path=str(DB_PATH),
        active_local_symbol=ACTIVE_LOCAL_SYMBOL,
        # Остальные поля оставляем дефолтными в dataclass
    )
    collector = BarsCollector(ib=ib_svc.ib, cfg=bars_cfg, logger=log)

    # --- Асинхронные задачи ---
    tasks: list[asyncio.Task] = []

    # Поддержание соединения (автореконнект, бипы и т.п.)
    tasks.append(asyncio.create_task(ib_svc.monitor_forever(stop_event), name="ib_guard"))

    # Вотчер портфеля
    tasks.append(asyncio.create_task(watcher.start(stop_event), name="portfolio_watch"))

    # Сборщик баров (создаст БД/таблицу если нужно, подтянет историю и дальше будет дозаливать)
    tasks.append(asyncio.create_task(collector.run(stop_event), name="bars_collector"))

    # Стартовая служебная метка в телеграм (по желанию пользователя)
    if TELEGRAM.enabled_logs:
        try:
            await tg.send_text(
                TELEGRAM.chat_id_logs,
                f"🤖 Робот запущен. Актив: {ACTIVE_LOCAL_SYMBOL}. БД: {DB_PATH.as_posix()}",
            )
        except Exception:
            log.exception("Не удалось отправить стартовое сообщение в Telegram")

    # Ожидаем завершение stop_event и всех задач
    try:
        await stop_event.wait()
    finally:
        # Мягко остановим все задачи
        for t in tasks:
            t.cancel()
        # Дадим задачам время схлопнуться
        await asyncio.gather(*tasks, return_exceptions=True)

        # Корректно закрываем соединение и телеграм
        try:
            await ib_svc.disconnect()
        except Exception:
            log.exception("Ошибка при отключении IB")

        try:
            await tg.aclose()
        except Exception:
            log.exception("Ошибка при закрытии Telegram клиента")

        log.info("✅ Робот завершил работу корректно.")


def _install_signal_handlers(stop_event: asyncio.Event, log: logging.Logger) -> None:
    def _stop(*_: object) -> None:
        # Идемпотентно выставляем флаг остановки
        if not stop_event.is_set():
            log.info("🛑 Получен сигнал на остановку, завершаю...")
            stop_event.set()

    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, lambda *_: _stop())
            except Exception:
                # На Windows SIGTERM может отсутствовать — это ок
                pass
    # PyCharm/Windows иногда шлет SIGBREAK
    if hasattr(signal, "SIGBREAK"):
        try:
            signal.signal(signal.SIGBREAK, lambda *_: _stop())
        except Exception:
            pass


def main() -> None:
    log = setup_logging()
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event, log)
    asyncio.run(run_all(stop_event))


if __name__ == "__main__":
    main()
