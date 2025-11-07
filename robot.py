# robot.py — единая точка входа. Только оркестрация задач.

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

from core.config import LOGGING, IB_CONFIG, TELEGRAM
from core.telegram import TelegramClient
from core.ib_connection import IBConnectionService
from core.portfolio_watch import PortfolioWatcher
from core.bars_collector import BarsCollector, BarsCollectorConfig

# --- базовая настройка логгера из config ---
_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}
logging.basicConfig(
    level=_LEVELS.get(str(LOGGING.level).upper(), logging.INFO),
    format=str(LOGGING.fmt),
)
log = logging.getLogger("robot")

# --- константы проекта (при желании перенесёте в core.config) ---
PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / "data" / "ib_bars.sqlite"
ACTIVE_LOCAL_SYMBOL = "MNQZ5"  # рабочий фьючерс

# --- служебный фон: «маяк» на начало часа ---
async def hourly_beacon(tg: TelegramClient, stop: asyncio.Event) -> None:
    """
    Раз в час шлём отметку о начале часа (UTC).
    """
    try:
        while not stop.is_set():
            now = datetime.now(timezone.utc)
            nxt = (now.replace(minute=0, second=0, microsecond=0)
                   + timedelta(hours=1))
            timeout = (nxt - now).total_seconds()
            try:
                await asyncio.wait_for(stop.wait(), timeout=timeout)
                break
            except asyncio.TimeoutError:
                pass

            msg = f"⏰ Начало часа: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
            if getattr(TELEGRAM, "enabled_logs", True):
                await tg.post_logs(msg)
            log.info(msg)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Ошибка в hourly_beacon")


async def run_all(stop: asyncio.Event) -> None:
    # 1) Телеграм-клиент (токен/чаты берёт из core.config)
    tg = TelegramClient()

    # 2) Сервис соединения с IB
    ib_service = IBConnectionService(IB_CONFIG)
    ib = ib_service.ib

    # 3) Наблюдение портфеля
    watcher = PortfolioWatcher(ib, tg, log)

    # 4) Сборщик 5-сек баров
    bars_cfg = BarsCollectorConfig(
        db_path=DB_PATH,
        active_local_symbol=ACTIVE_LOCAL_SYMBOL,
    )
    bars = BarsCollector(ib, bars_cfg, logger=log)

    tasks = [
        asyncio.create_task(ib_service.monitor_forever(stop), name="ib_monitor"),
        asyncio.create_task(watcher.start(), name="portfolio_watch"),  # без stop — как в вашей сигнатуре
        asyncio.create_task(bars.run(stop), name="bars_collector"),
        asyncio.create_task(hourly_beacon(tg, stop), name="hourly_beacon"),
    ]

    log.info("🚀 Робот стартует. Лог-уровень: %s", LOGGING.level)
    if getattr(TELEGRAM, "enabled_logs", True):
        await tg.post_logs("🤖 Робот запущен.")

    try:
        await asyncio.gather(*tasks)
    finally:
        # Отмена и дожидание задач
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Корректное отключение от IB
        try:
            await ib_service.disconnect()
        except Exception:
            log.exception("Ошибка при отключении от IB")

        if getattr(TELEGRAM, "enabled_logs", True):
            await tg.post_logs("✅ Робот завершил работу корректно.")
        log.info("✅ Робот завершил работу корректно.")


def main() -> None:
    stop = asyncio.Event()
    try:
        asyncio.run(run_all(stop))
    except KeyboardInterrupt:
        log.info("🛑 Получен сигнал на остановку, завершаю...")
        stop.set()
    except Exception:
        log.exception("Критическая ошибка верхнего уровня")


if __name__ == "__main__":
    main()
