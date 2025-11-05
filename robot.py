from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict
from zoneinfo import ZoneInfo

from ib_insync import IB, PortfolioItem
from core.config import LOGGING, IB_CONFIG, TELEGRAM
from core.telegram import TelegramClient

# Локальная тайм-зона
TZ = ZoneInfo("Europe/Moscow")


# ------------------------------- утилиты логирования -------------------------------

def _setup_logging() -> None:
    level = LOGGING.level.upper()
    logging.basicConfig(
        level=level,
        format=LOGGING.fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _now() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------- IB: сводка при старте ----------------------------

def _compose_startup_snapshot(ib: IB) -> str:
    """
    Короткая служебная сводка из кэша ib_insync (plain-text).
    """
    acc_vals = ib.accountValues() or []
    positions = ib.positions() or []
    portfolio = ib.portfolio() or []

    accounts = sorted({av.account for av in acc_vals}) or ["?"]

    def _get(tag: str, currency: str | None = None) -> str:
        for av in acc_vals:
            if av.tag == tag and (currency is None or av.currency == currency):
                return str(av.value)
        return "n/a"

    # Базовые метрики
    netliq_usd = _get("NetLiquidation", "USD")
    avail_usd = _get("AvailableFunds", "USD")
    cash_usd = _get("TotalCashBalance", "USD")
    cash_eur = _get("TotalCashBalance", "EUR")

    # Короткий перечень позиций
    pos_lines = []
    for p in positions[:10]:
        sym = getattr(p.contract, "localSymbol", None) or getattr(p.contract, "symbol", "?")
        qty = p.position
        pos_lines.append(f"- {sym}: {qty:g}")

    # Итоговый текст
    lines = [
        "Служебная сводка при старте:",
        f"Аккаунты: {', '.join(accounts)}",
        f"NetLiq USD: {netliq_usd}",
        f"AvailableFunds USD: {avail_usd}",
        f"Cash USD: {cash_usd} | EUR: {cash_eur}",
        f"Позиций: {len(positions)}; в портфеле записей: {len(portfolio)}",
    ]
    if pos_lines:
        lines.append("Топ позиций:")
        lines.extend(pos_lines)
    return "\n".join(lines)


# ------------------------------- Portfolio Watcher --------------------------------

class PortfolioWatcher:
    """
    Следит за открытием/закрытием позиций и отправляет уведомления в телеграм.
    Без sync-методов ib_insync, без reqAccountUpdates().
    """

    def __init__(self, ib: IB, tg: TelegramClient, logger: logging.Logger) -> None:
        self.ib = ib
        self.tg = tg
        self.log = logger
        self._baseline: Dict[int, float] = {}  # conId -> qty
        self._attached = False

    def _on_update_portfolio(self, item: PortfolioItem) -> None:
        c = item.contract
        con_id = getattr(c, "conId", None)
        if con_id is None:
            return
        prev_qty = self._baseline.get(con_id, 0.0)
        new_qty = float(getattr(item, "position", 0.0))

        # Если это первое появление инструмента в baseline — просто фиксируем без уведомлений
        if con_id not in self._baseline:
            self._baseline[con_id] = new_qty
            return

        # Открытие позиции (0 -> != 0)
        if prev_qty == 0.0 and new_qty != 0.0:
            side = "LONG" if new_qty > 0 else "SHORT"
            sym = getattr(c, "localSymbol", None) or getattr(c, "symbol", "?")
            upnl = getattr(item, "unrealizedPNL", 0.0)
            self.log.info(f"Открыта позиция: {sym} {side} qty={new_qty:g}")
            if TELEGRAM.enabled_logs:
                asyncio.create_task(
                    self.tg.send_text(f"📈 Открыта позиция: {sym} {side} qty={new_qty:g}\n"
                                      f"uPnL: {upnl:+.2f}")
                )

        # Закрытие позиции (!= 0 -> 0)
        if prev_qty != 0.0 and new_qty == 0.0:
            side = "LONG" if prev_qty > 0 else "SHORT"
            sym = getattr(c, "localSymbol", None) or getattr(c, "symbol", "?")
            rpnl = getattr(item, "realizedPNL", 0.0)
            self.log.info(f"Закрыта позиция: {sym} {side} qty=0")
            if TELEGRAM.enabled_logs:
                asyncio.create_task(
                    self.tg.send_text(f"📉 Закрыта позиция: {sym} ({side})\n"
                                      f"rPnL: {rpnl:+.2f}")
                )

        # Обновляем baseline
        self._baseline[con_id] = new_qty

    async def start(self) -> None:
        if self._attached:
            return
        # Начальный снимок без уведомлений
        for it in self.ib.portfolio():
            c = it.contract
            con_id = getattr(c, "conId", None)
            if con_id is not None:
                self._baseline[con_id] = float(getattr(it, "position", 0.0))

        # Подписка на события портфеля
        self.ib.updatePortfolioEvent += self._on_update_portfolio
        self._attached = True
        self.log.info("🔗 PortfolioWatch: connected()")

    async def stop(self) -> None:
        if not self._attached:
            return
        try:
            self.ib.updatePortfolioEvent -= self._on_update_portfolio
        finally:
            self._attached = False
            self.log.info("🔌 PortfolioWatch: disconnected()")


# ------------------------------- задачи: часовые маяки ----------------------------

async def _hour_beacons(ib: IB, tg: TelegramClient) -> None:
    if not TELEGRAM.enabled_logs:
        return
    # ждём ближайший верх часа
    now = datetime.now(TZ)
    next_top = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    await asyncio.sleep((next_top - now).total_seconds())

    while True:
        # Сформируем короткую сводку на верх часа из кэша
        snapshot = _compose_startup_snapshot(ib)
        await tg.send_text(f"🕛 Начало часа: <b>{_now()}</b>\n\n{snapshot}")
        # следующий верх часа
        await asyncio.sleep(3600)


# ------------------------------------ main ----------------------------------------

async def amain() -> None:
    _setup_logging()
    log = logging.getLogger("robot")
    log.info("🚀 Робот стартует. Лог-уровень: %s", LOGGING.level)

    tg_client = TelegramClient()  # читает TELEGRAM из core.config
    ib = IB()

    retry = IB_CONFIG.base_retry_delay
    while True:
        log.info("🔗 Подключаюсь к IB ...")
        try:
            await ib.connectAsync(IB_CONFIG.host, IB_CONFIG.port, clientId=IB_CONFIG.client_id)
            log.info("✅ Подключено к IB %s:%s (clientId=%s) в %s",
                     IB_CONFIG.host, IB_CONFIG.port, IB_CONFIG.client_id, _now())

            # watchers & маяки
            watcher = PortfolioWatcher(ib, tg_client, log)
            await watcher.start()
            # стартовый снимок в лог-канал (по желанию)
            if TELEGRAM.enabled_logs:
                await tg_client.send_text("📸 " + _compose_startup_snapshot(ib))

            # маяки часа в фоне
            beacons_task = asyncio.create_task(_hour_beacons(ib, tg_client))

            # рабочий цикл до разрыва
            try:
                while ib.isConnected():
                    await asyncio.sleep(IB_CONFIG.health_check_period)
            finally:
                beacons_task.cancel()
                await watcher.stop()

            log.info("🔌 Отключаюсь от IB ...")
            ib.disconnect()
            # сбрасываем экспоненту, раз подключались успешно
            retry = IB_CONFIG.base_retry_delay

        except Exception as e:
            log.exception("Ошибка подключения/работы: %s", e)

        # реконнект с бэкоффом
        await asyncio.sleep(retry)
        retry = min(retry * 2, IB_CONFIG.max_retry_delay)


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
