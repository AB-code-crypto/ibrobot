from __future__ import annotations
import asyncio
import logging
from core.config import IBConfig
from ib_insync import IB, util

log = logging.getLogger(__name__)


class IBConnectionError(Exception):
    pass


class IBConnectionService:
    """
    Асинхронное соединение с IB:
      • первичный коннект
      • удержание и реконнект в одном цикле мониторинга
    Исключения наружу — без "тихого" падения.
    """

    def __init__(self, cfg: IBConfig):
        self.cfg = cfg
        self.ib = IB()

    def get_ib(self) -> IB:
        return self.ib

    async def connect_initial(self) -> None:
        util.logToConsole(True)  # максимум логов от ib_insync в консоль
        log.info("▶ Первичное подключение к IB %s:%s (clientId=%s)",
                 self.cfg.host, self.cfg.port, self.cfg.client_id)
        await self._connect_once()
        log.info("✔ Первичное подключение установлено.")

    async def disconnect(self) -> None:
        if self.ib.isConnected():
            log.info("⏹ Отключаюсь от IB ...")
            try:
                self.ib.disconnect()
            finally:
                await asyncio.sleep(0.05)
            log.info("✔ Соединение закрыто.")

    async def monitor_forever(self, stop_event: asyncio.Event) -> None:
        delay = self.cfg.base_retry_delay
        log.info("🩺 Монитор соединения запущен (период=%.1fs).", self.cfg.health_check_period)

        while not stop_event.is_set():
            if not self.ib.isConnected():
                log.warning("⚠ Обнаружено отключение. Пытаюсь переподключиться ...")
                try:
                    await self._connect_once()
                    delay = self.cfg.base_retry_delay
                    log.info("✔ Успешно переподключились.")
                except Exception as e:
                    log.error("❌ Реконнект не удался: %s", e, exc_info=True)
                    delay = min(delay * 1.618, self.cfg.max_retry_delay)
                    log.info("⏳ Следующая попытка через %.2f сек.", delay)
                    await asyncio.sleep(delay)
                    continue

            await asyncio.sleep(self.cfg.health_check_period)

        log.info("🛑 Монитор соединения остановлен (stop_event).")

    async def _connect_once(self) -> None:
        if self.ib.isConnected():
            await self._disconnect_safely()

        log.info("🔗 Подключаюсь к IB %s:%s (clientId=%s) ...",
                 self.cfg.host, self.cfg.port, self.cfg.client_id)

        await self.ib.connectAsync(
            self.cfg.host,
            self.cfg.port,
            clientId=self.cfg.client_id,
            timeout=5.0
        )

        if not self.ib.isConnected():
            raise IBConnectionError("connectAsync завершился без исключения, но соединение не установлено.")

        sv = getattr(self.ib.serverVersion(), "value", self.ib.serverVersion())
        log.info("✅ Подключено: serverVersion=%s, connectionTime=%s",
                 sv, self.ib.connectionTime())

    async def _disconnect_safely(self) -> None:
        try:
            self.ib.disconnect()
        finally:
            await asyncio.sleep(0.05)
