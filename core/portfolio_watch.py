from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from ib_insync import IB, PortfolioItem, Position

from core.config import TELEGRAM
from core.telegram import TelegramClient

log = logging.getLogger("portfolio_watch")


@dataclass
class _PosState:
    qty: float = 0.0


def _fmt_money(x: Optional[float]) -> str:
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return str(x)


class PortfolioWatcher:
    """
    Отслеживает изменения позиций и шлёт сообщение в телеграм,
    когда позиция открылась (0 -> !=0) или закрылась (!=0 -> 0).
    Реализовано на событиях IB.updatePortfolioEvent; при реконнекте подписка возобновляется.
    """

    def __init__(
            self,
            ib: IB,
            tg: TelegramClient,
            chat_id_logs: int,
            poll_snapshot_on_connect: bool = True,
    ) -> None:
        import logging
        self.logger = logging.getLogger("robot")
        self.ib = ib
        self.tg = tg
        self.chat_id = chat_id_logs
        self._pos: Dict[int, _PosState] = {}
        self._subscribed = False
        self._handlers_attached = False
        self._stop = asyncio.Event()
        self._poll_snapshot_on_connect = poll_snapshot_on_connect

    async def start(self) -> None:
        """
        Запускает вотчер. Возвращается, когда вызван stop().
        """
        # Привязываем обработчики один раз
        if not self._handlers_attached:
            self.ib.updatePortfolioEvent += self._handle_update_portfolio
            self.ib.connectedEvent += self._handle_connected
            self.ib.disconnectedEvent += self._handle_disconnected
            self._handlers_attached = True

        # Если уже подключены — инициализация
        if self.ib.isConnected():
            await self._on_connected_async()

        # Лёгкий цикл для поддержания подписки
        while not self._stop.is_set():
            if self.ib.isConnected() and not self._subscribed:
                await self._ensure_subscribed()
            await asyncio.sleep(1.0)

    def stop(self) -> None:
        self._stop.set()

    # --- event wrappers (синхронные) ---
    def _handle_connected(self, *_):
        asyncio.create_task(self._on_connected_async())

    def _handle_disconnected(self, *_):
        log.info("🔌 PortfolioWatcher: disconnected()")
        self._subscribed = False

    def _handle_update_portfolio(self, item: PortfolioItem):
        asyncio.create_task(self._on_update_portfolio_async(item))

    # --- async impl ---

    async def _ensure_subscribed(self) -> None:
        accounts = list(self.ib.managedAccounts)
        if not accounts:
            # Иногда список приходит не сразу — ждём чуть-чуть
            for _ in range(10):
                await asyncio.sleep(0.2)
                if self.ib.managedAccounts:
                    accounts = list(self.ib.managedAccounts)
                    break

        account = accounts[0] if accounts else ""
        try:
            self.ib.reqAccountUpdates(True, account)
            self._subscribed = True
            log.info(f"📡 PortfolioWatcher: подписан на обновления портфеля для '{account or 'default'}'")
        except Exception as e:
            log.exception("Не удалось подписаться на обновления портфеля: %s", e)

    async def _snapshot_positions(self) -> None:
        """
        Снимаем стартовый снимок позиций — чтобы не спамить «ОТКРЫТА»
        для уже существующих позиций при старте/реконнекте.
        """
        try:
            positions = await self.ib.reqPositionsAsync()
        except Exception as e:
            log.warning("Не удалось получить стартовый снимок позиций: %s", e)
            return

        self._pos.clear()
        for p in positions:  # type: Position
            cid = int(getattr(p.contract, "conId"))
            self._pos[cid] = _PosState(qty=float(p.position))

        if positions:
            log.info("📸 Стартовый снимок позиций: %d инструмент(ов).", len(positions))
        else:
            log.info("📸 Стартовый снимок позиций: пусто.")

    async def _on_connected_async(self) -> None:
        log.info("🔗 PortfolioWatcher: connected()")
        self._subscribed = False
        if self._poll_snapshot_on_connect:
            await self._snapshot_positions()
        await self._ensure_subscribed()

    def _is_zero(self, x: float) -> bool:
        return abs(x) < 1e-8

    async def _on_update_portfolio_async(self, item: PortfolioItem) -> None:
        """
        Обработчик событий портфеля.
        """
        try:
            c = item.contract
            cid = int(getattr(c, "conId"))
            prev = self._pos.get(cid, _PosState()).qty
            now = float(item.position or 0.0)

            opened = self._is_zero(prev) and not self._is_zero(now)
            closed = not self._is_zero(prev) and self._is_zero(now)

            # Обновляем состояние заранее
            self._pos[cid] = _PosState(qty=now)

            if opened:
                side = "LONG" if now > 0 else "SHORT"
                msg_lines = [
                    "🟢 ОТКРЫТА ПОЗИЦИЯ",
                    f"Инструмент: {getattr(c, 'localSymbol', getattr(c, 'symbol', 'N/A'))}",
                    f"Сторона: {side}",
                    f"Кол-во: {now}",
                ]
                avg = getattr(item, "averageCost", None)
                price = getattr(item, "marketPrice", None)
                mv = getattr(item, "marketValue", None)
                if avg not in (None, 0):
                    msg_lines.append(f"Средняя цена: {_fmt_money(avg)}")
                if price not in (None, 0):
                    msg_lines.append(f"Рыночная: {_fmt_money(price)}")
                if mv not in (None, 0):
                    msg_lines.append(f"Стоимость: ${_fmt_money(mv)}")
                await self._send("\n".join(msg_lines))
                log.info("Открыта позиция: %s %s qty=%s",
                         getattr(c, 'localSymbol', getattr(c, 'symbol', 'N/A')), side, now)

            elif closed:
                side = "LONG" if prev > 0 else "SHORT"
                msg_lines = [
                    "🔴 ЗАКРЫТА ПОЗИЦИЯ",
                    f"Инструмент: {getattr(c, 'localSymbol', getattr(c, 'symbol', 'N/A'))}",
                    f"Пред. сторона: {side}",
                    f"Закрытый объём: {prev}",
                ]
                rpn = getattr(item, "realizedPNL", None) or getattr(item, "realizedPnL", None)
                if rpn not in (None, 0):
                    msg_lines.append(f"Realized PnL: ${_fmt_money(rpn)}")
                await self._send("\n".join(msg_lines))
                log.info("Закрыта позиция: %s prev_qty=%s",
                         getattr(c, 'localSymbol', getattr(c, 'symbol', 'N/A')), prev)

        except Exception as e:
            log.exception("Ошибка в обработчике обновлений портфеля: %s", e)

    async def _send(self, text: str) -> None:
        if TELEGRAM.enabled_logs:
            try:
                await self.tg.send_text(TELEGRAM.chat_id_logs, text)
            except Exception as e:
                log.warning("Не удалось отправить в телеграм: %s", e)
