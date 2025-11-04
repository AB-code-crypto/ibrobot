import asyncio
import logging
import signal

from infra.ib_connection import IBConnectionService
from settings import IB_CONFIG, LOGGING

# Маппинг уровней, чтобы не использовать getattr
_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


def _setup_logging():
    level = _LEVELS.get(LOGGING.level.upper(), logging.DEBUG)
    logging.basicConfig(level=level, format=LOGGING.fmt)
    logging.captureWarnings(True)
    logging.getLogger(__name__).info("🚀 Робот стартует. Лог-уровень: %s", LOGGING.level)


async def _run():
    _setup_logging()

    svc = IBConnectionService(IB_CONFIG)

    stop_event = asyncio.Event()

    def _on_stop(*_):
        logging.getLogger(__name__).info("🧹 Сигнал остановки получен, завершаю ...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _on_stop)
        except NotImplementedError:
            pass  # Windows fallback

    # 1) Первичное подключение — если ошибка, процесс падает (как и нужно)
    await svc.connect_initial()

    # 2) Монитор соединения — исключения наружу (TaskGroup)
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(svc.monitor_forever(stop_event), name="ib-monitor")
            await stop_event.wait()
    finally:
        await svc.disconnect()
        logging.getLogger(__name__).info("✅ Робот завершил работу корректно.")


def main():
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        print("\n^C — остановлено пользователем.")
    except Exception as e:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
        logging.getLogger(__name__).exception("💥 Критическая ошибка робота: %s", e)
        raise


if __name__ == "__main__":
    main()
