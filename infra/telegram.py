from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.error
import urllib.request
from typing import Optional

log = logging.getLogger(__name__)


class TelegramClient:
    """
    Лёгкий async-отправитель через стандартную библиотеку.
    Делает HTTP POST в sendMessage через to_thread(), чтобы не блокировать event loop.
    """
    def __init__(self, bot_token: str, timeout: float = 7.0, parse_mode: str = "HTML"):
        if not bot_token:
            raise ValueError("Telegram bot token is empty")
        self._url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._timeout = timeout
        self._parse_mode = parse_mode

    def _post_sync(self, payload: dict) -> None:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._url,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            # читаем, чтобы не держать соединение
            _ = resp.read()

    async def send_text(self, chat_id: int, text: str, disable_preview: bool = True) -> None:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": self._parse_mode,
            "disable_web_page_preview": disable_preview,
        }
        await asyncio.to_thread(self._post_sync, payload)


class AsyncTelegramLogHandler(logging.Handler):
    """
    Асинхронный лог-хендлер:
      • собирает записи в asyncio.Queue
      • в фоне отправляет их в Телеграм через TelegramClient
    """
    def __init__(self, client: TelegramClient, chat_id: int, level: int = logging.INFO,
                 max_queue: int = 1000, silent_exceptions: bool = False):
        super().__init__(level=level)
        self.client = client
        self.chat_id = chat_id
        self.queue: asyncio.Queue[Optional[logging.LogRecord]] = asyncio.Queue(maxsize=max_queue)
        self._task: Optional[asyncio.Task] = None
        self.silent_exceptions = silent_exceptions

    def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._pump(), name="tg-log-pump")

    async def stop(self) -> None:
        await self.queue.put(None)
        if self._task:
            try:
                await self._task
            finally:
                self._task = None

    async def _pump(self) -> None:
        # простая антиперегрузочная пауза
        last_sent = 0.0
        try:
            while True:
                rec = await self.queue.get()
                if rec is None:
                    break
                msg = self.format(rec)
                # минимальная защита от флуда
                now = time.monotonic()
                if now - last_sent < 0.05:
                    await asyncio.sleep(0.05)
                try:
                    await self.client.send_text(self.chat_id, msg)
                except Exception as e:
                    if self.silent_exceptions:
                        log.debug("Telegram log send failed: %s", e, exc_info=True)
                    else:
                        log.error("Telegram log send failed: %s", e, exc_info=True)
                finally:
                    last_sent = time.monotonic()
                    self.queue.task_done()
        except asyncio.CancelledError:
            pass

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except asyncio.QueueFull:
            # если очередь переполнена — не блокируем выполнение робота
            pass


class OrdersNotifier:
    """
    Отправитель торговых приказов в отдельный канал.
    Предусмотрены короткие методы: buy/sell/profit.
    """
    def __init__(self, client: TelegramClient, chat_id: int):
        self.client = client
        self.chat_id = chat_id

    async def buy(self, symbol: str, qty: float, price: float, note: str = "") -> None:
        text = f"🟢 <b>BUY</b> {symbol}  qty=<b>{qty}</b>  price=<b>{price}</b>\n{note}".strip()
        await self.client.send_text(self.chat_id, text)

    async def sell(self, symbol: str, qty: float, price: float, note: str = "") -> None:
        text = f"🔴 <b>SELL</b> {symbol}  qty=<b>{qty}</b>  price=<b>{price}</b>\n{note}".strip()
        await self.client.send_text(self.chat_id, text)

    async def profit(self, symbol: str, pnl_abs: float, pnl_pct: float, note: str = "") -> None:
        sign = "🟩" if pnl_abs >= 0 else "🟥"
        text = f"{sign} <b>PnL</b> {symbol}: <b>{pnl_abs:.2f}</b> ({pnl_pct:.2f}%)\n{note}".strip()
        await self.client.send_text(self.chat_id, text)
