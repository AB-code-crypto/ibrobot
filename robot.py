"""
Robot runner with IB connection, portfolio watcher, hour beacons,
and BarsCollector integration (5s bars to SQLite).
Assumptions:
- core.config provides IB_CONFIG, LOGGING, TELEGRAM (no env/getattr).
- core.telegram provides TelegramClient() (no-arg) with async post(text).
- core.bars_collector provides BarsCollector(symbol:str, db_path:Path).
If bars_collector is missing, we log a warning and continue without it.
"""

import asyncio
import logging
import signal
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ib_insync import IB

from core.bars_collector import BarsCollector
from core.config import IB_CONFIG, LOGGING, TELEGRAM
from core.portfolio_watch import PortfolioWatcher
from core.telegram import TelegramClient

# --- Timezone ----------------------------------------------------------------
TZ = ZoneInfo("Europe/Moscow")

# --- Logging setup ------------------------------------------------------------
_level_map = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}
lvl = _level_map.get(str(LOGGING.level).upper(), logging.INFO)
logging.basicConfig(level=lvl, format=LOGGING.fmt)
log = logging.getLogger("robot")

# --- Graceful shutdown --------------------------------------------------------
class Shutdown:
    def __init__(self):
        self._event = asyncio.Event()
    def set(self):
        self._event.set()
    async def wait(self):
        await self._event.wait()

shutdown = Shutdown()

def _install_signal_handlers():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, shutdown.set)
        except NotImplementedError:
            # Windows/PyCharm fallback
            pass

# --- Hourly beacons -----------------------------------------------------------
async def hour_beacons(tg: TelegramClient):
    last_hour = None
    while not shutdown._event.is_set():
        now_hour = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
        if now_hour != last_hour:
            last_hour = now_hour
            msg = f"⏱ Начало часа: {now_hour:%Y-%m-%d %H:%M}"
            log.info(msg)
            if TELEGRAM.enabled_logs:
                try:
                    await tg.post(msg)
                except Exception:
                    pass
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pass

# --- IB guard: connect / reconnect / notify ----------------------------------
async def ib_guard(ib: IB, tg: TelegramClient, bars: BarsCollector, watcher: PortfolioWatcher):
    backoff = 2.0
    while not shutdown._event.is_set():
        try:
            log.info("🔗 Подключаюсь к IB ...")
            await ib.connectAsync(IB_CONFIG.host, IB_CONFIG.port, clientId=IB_CONFIG.client_id)
            log.info("✅ Подключено к IB %s:%s (clientId=%s) в %s",
                     IB_CONFIG.host, IB_CONFIG.port, IB_CONFIG.client_id,
                     datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
            if TELEGRAM.enabled_logs:
                try:
                    await tg.post("✅ Подключился к IB")
                except Exception:
                    pass

            await bars.on_connected(ib)
            await watcher.on_connected()

            # Wait for disconnect/shutdown
            while ib.isConnected() and not shutdown._event.is_set():
                await asyncio.sleep(0.5)

            await watcher.on_disconnected()
            await bars.on_disconnected()

            if shutdown._event.is_set():
                break

            log.info("🔌 Соединение потеряно, пробую восстановить...")
            if TELEGRAM.enabled_logs:
                try:
                    await tg.post("🔌 Соединение потеряно, пробую восстановить...")
                except Exception:
                    pass

            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.7, 30.0)

        except Exception as e:
            log.error("Ошибка подключения/работы: %s", e, exc_info=False)
            if TELEGRAM.enabled_logs:
                try:
                    await tg.post(f"⚠️ Ошибка подключения: {e}")
                except Exception:
                    pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.7, 30.0)

# --- Bars collector bootstrap -------------------------------------------------
@dataclass(frozen=True)
class BarsOptions:
    symbol: str
    db_path: Path

def _discover_db_path() -> Path:
    try:
        from core.config import HISTORY  # type: ignore
        p = Path(getattr(HISTORY, "db_path"))
        if p:
            return p
    except Exception:
        pass
    return Path("./history/history.sqlite")

def parse_args():
    import argparse
    ap = argparse.ArgumentParser(description="IB robot (with BarsCollector)")
    ap.add_argument("--symbol", default=getattr(IB_CONFIG, "active_symbol", "MNQZ5"),
                    help="Active contract symbol, e.g. MNQZ5")
    ap.add_argument("--db", default=str(_discover_db_path()), help="SQLite DB path")
    return ap.parse_args()

# --- Main --------------------------------------------------------------------
async def amain():
    _install_signal_handlers()

    args = parse_args()
    log.info("🚀 Робот стартует. Лог-уровень: %s", LOGGING.level)

    tg = TelegramClient()
    ib = IB()

    # Bars collector
    bars = BarsCollector(symbol=args.symbol, db_path=Path(args.db))
    try:
        await bars.start()
    except Exception:
        pass

    watcher = PortfolioWatcher(ib, tg, log)

    # Run tasks
    tasks = [
        asyncio.create_task(ib_guard(ib, tg, bars, watcher), name="ib_guard"),
        asyncio.create_task(hour_beacons(tg), name="hour_beacons"),
        asyncio.create_task(watcher.run(), name="portfolio_watcher"),
    ]

    try:
        await shutdown.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await bars.stop()
        except Exception:
            pass
        if ib.isConnected():
            log.info("🔌 Отключаюсь от IB ...")
            ib.disconnect()
        log.info("✅ Робот завершил работу корректно.")

def main():
    asyncio.run(amain())

if __name__ == "__main__":
    main()
