import asyncio
import logging
import signal

from core.config import LOGGING, IB_CONFIG, TELEGRAM
from infra.ib_connection import IBConnectionService
from infra.telegram import TelegramClient, AsyncTelegramLogHandler, OrdersNotifier

_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


def _setup_logging() -> None:
    level = _LEVELS.get(LOGGING.level.upper(), logging.DEBUG)
    logging.basicConfig(level=level, format=LOGGING.fmt)
    logging.captureWarnings(True)
    logging.getLogger(__name__).info("🚀 Робот стартует. Лог-уровень: %s", LOGGING.level)


async def _install_telegram() -> tuple[AsyncTelegramLogHandler | None, OrdersNotifier | None]:
    """
    Подключает телеграм-логгер (если включён) и готовит отправитель приказов.
    Вся логика отправки — внутри infra.telegram.*
    """
    # Логи → канал логов
    tg_handler: AsyncTelegramLogHandler | None = None
    orders: OrdersNotifier | None = None

    if TELEGRAM.enabled_logs and TELEGRAM.bot_token and TELEGRAM.chat_id_logs:
        client = TelegramClient(TELEGRAM.bot_token, timeout=7.0, parse_mode="HTML")
        tg_handler = AsyncTelegramLogHandler(
            client=client,
            chat_id=TELEGRAM.chat_id_logs,
            level=_LEVELS.get(LOGGING.level.upper(), logging.DEBUG),
            silent_exceptions=False,
        )
        tg_handler.setFormatter(logging.Formatter(LOGGING.fmt))
        logging.getLogger().addHandler(tg_handler)
        tg_handler.start()
        logging.getLogger(__name__).info("✈️ Телеграм-логгер активирован: чат %s", TELEGRAM.chat_id_logs)
        # Приказы → отдельный канал (включаем только если разрешено)
        if TELEGRAM.enabled_trade and TELEGRAM.chat_id_trade:
            orders = OrdersNotifier(client, TELEGRAM.chat_id_trade)
            logging.getLogger(__name__).info("🧾 Канал приказов активирован: чат %s", TELEGRAM.chat_id_trade)
    else:
        logging.getLogger(__name__).info("Телеграм-логгер отключён (см. core/config.py → TELEGRAM)")

    return tg_handler, orders


async def _run():
    _setup_logging()

    tg_handler, orders = await _install_telegram()  # orders пока не используем на шаге 1
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
            # Windows: сигналы частично, Ctrl+C поймаем как KeyboardInterrupt
            pass

    # 1) Первичное подключение (ошибка — наверх; "если падаем — падаем")
    await svc.connect_initial()

    # 2) Монитор соединения — исключения не теряем
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(svc.monitor_forever(stop_event), name="ib-monitor")
            await stop_event.wait()
    finally:
        await svc.disconnect()
        if tg_handler:
            await tg_handler.stop()
        logging.getLogger(__name__).info("✅ Робот завершил работу корректно.")


def main():
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        print("\n^C — остановлено пользователем.")
    except Exception as e:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
        )
        logging.getLogger(__name__).exception("💥 Критическая ошибка робота: %s", e)
        raise


if __name__ == "__main__":
    main()
