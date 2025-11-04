from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from ib_insync import IB, util

from core.config import IBConfig

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Backoff:
    base: float
    current: float
    max_: float

    def next(self) -> float:
        return min(self.current * 2, self.max_)

    def reset(self) -> float:
        return self.base


class IBConnectionService:
    """
    Модуль соединения с IBKR:
    - первичное подключение (connect_initial)
    - мониторинг и авто-реконнект (monitor_forever)
    - корректный disconnect()
    """

    def __init__(self, cfg: IBConfig):
        self.cfg = cfg
        self.ib = IB()
        util.logToConsole(True)  # подробный лог ib_insync в консоль
        self._backoff = _Backoff(
            base=cfg.base_retry_delay,
            current=cfg.base_retry_delay,
            max_=cfg.max_retry_delay,
        )

    # ---------------- Public API ----------------

    async def connect_initial(self) -> None:
        log.info("▶ Первичное подключение к IB %s:%s (clientId=%s)", self.cfg.host, self.cfg.port, self.cfg.client_id)
        await self._connect_once()
        # после успешного первичного — сбросить бэкофф на базовый
        self._backoff = _Backoff(
            base=self.cfg.base_retry_delay,
            current=self.cfg.base_retry_delay,
            max_=self.cfg.max_retry_delay,
        )

    async def monitor_forever(self, stop_event: asyncio.Event) -> None:
        """
        Периодически проверяем соединение. Если упало — реконнект с экспоненциальным бэкоффом.
        """
        period = max(0.5, float(self.cfg.health_check_period))
        while not stop_event.is_set():
            try:
                if not self.ib.isConnected():
                    delay = self._backoff.current
                    log.warning("⛔ Соединение потеряно. Повторная попытка через %.1f сек ...", delay)
                    await asyncio.sleep(delay)
                    try:
                        await self._connect_once()
                        self._backoff = _Backoff(
                            base=self.cfg.base_retry_delay,
                            current=self.cfg.base_retry_delay,
                            max_=self.cfg.max_retry_delay,
                        )
                    except Exception as e:
                        log.error("♻️ Реконнект не удался: %s", e, exc_info=True)
                        self._backoff = _Backoff(
                            base=self._backoff.base,
                            current=self._backoff.next(),
                            max_=self._backoff.max_,
                        )
                else:
                    await asyncio.sleep(period)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("monitor_forever: неожиданная ошибка: %s", e, exc_info=True)
                await asyncio.sleep(self._backoff.current)

    async def disconnect(self) -> None:
        try:
            if self.ib.isConnected():
                log.info("🔌 Отключаюсь от IB ...")
                # ib.disconnect() синхронный — уводим в поток
                await asyncio.to_thread(self.ib.disconnect)
                log.info("🔚 Соединение закрыто.")
        except Exception as e:
            log.error("Ошибка при отключении: %s", e, exc_info=True)

    # ---------------- Internal ----------------

    async def _connect_once(self) -> None:
        log.info("🔗 Подключаюсь к IB %s:%s (clientId=%s) ...", self.cfg.host, self.cfg.port, self.cfg.client_id)
        t0 = time.monotonic()
        # connectAsync — корреткно работает в asyncio
        await self.ib.connectAsync(
            host=self.cfg.host,
            port=self.cfg.port,
            clientId=self.cfg.client_id,
            timeout=10,  # сек
        )
        if not self.ib.isConnected():
            raise RuntimeError("Не удалось подключиться к IB (isConnected=False)")

        dt = (time.monotonic() - t0) * 1000.0
        # serverVersion/connectionTime — берём у client и безопасно
        sv = None
        try:
            sv = getattr(self.ib.client, "serverVersion", None)
        except Exception:
            sv = None

        ctime = None
        try:
            if hasattr(self.ib.client, "connectionTime"):
                ctime = self.ib.client.connectionTime()  # строка с временем от сервера
        except Exception:
            ctime = None

        log.info(
            "✅ Подключено за %.0f ms. ServerVersion=%s, ConnectionTime=%s",
            dt,
            sv if sv is not None else "n/a",
            ctime if ctime else "n/a",
        )
