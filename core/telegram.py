from __future__ import annotations

import asyncio
import logging
from typing import Iterable, Optional
from urllib import parse, request, error

from core.config import TELEGRAM

log = logging.getLogger(__name__)


class TelegramClient:
    """
    Минимальный и надёжный клиент для отправки plain-text сообщений
    в два канала: логи и торговые алерты. Никакого parse_mode.
    Ошибки HTTP не пробрасываются наружу — логируем и продолжаем.
    """

    _MAX_LEN = 4000  # запас до лимита 4096

    def __init__(self, *, timeout: int = 10) -> None:
        self._timeout = timeout
        self._base_url = f"https://api.telegram.org/bot{TELEGRAM.bot_token}"
        self._enabled_logs = TELEGRAM.enabled_logs
        self._enabled_trade = TELEGRAM.enabled_trade
        self._chat_logs = str(TELEGRAM.chat_id_logs)
        self._chat_trade = str(TELEGRAM.chat_id_trade)

    # --- public sync API -------------------------------------------------

    def send_text_sync(self, text: str, *, to: str = "logs") -> bool:
        """
        Отправка синхронно. to: "logs" | "trade".
        Возвращает True/False — получилось ли отправить все куски.
        """
        chat_id, enabled = self._resolve_destination(to)
        if not enabled:
            return False

        ok_all = True
        for chunk in self._chunk(text):
            ok = self._post_send_message(chat_id, chunk)
            if not ok:
                ok_all = False
                # не роняем процесс, просто отмечаем неудачу и идём дальше
        return ok_all

    # --- async sugar -----------------------------------------------------

    async def send_text(self, text: str, *, to: str = "logs") -> bool:
        """
        Асинхронный враппер над send_text_sync через to_thread.
        """
        return await asyncio.to_thread(self.send_text_sync, text, to=to)

    # --- internals -------------------------------------------------------

    def _resolve_destination(self, to: str) -> tuple[str, bool]:
        if to == "logs":
            return self._chat_logs, self._enabled_logs
        if to == "trade":
            return self._chat_trade, self._enabled_trade
        raise ValueError(f"unknown destination: {to!r}")

    def _chunk(self, text: str, limit: int = _MAX_LEN) -> Iterable[str]:
        """
        Разбивает текст на безопасные куски ≤ limit.
        Пытается резать по переводу строки.
        """
        if len(text) <= limit:
            yield text
            return

        i = 0
        n = len(text)
        while i < n:
            j = min(n, i + limit)
            nl = text.rfind("\n", i, j)
            if nl > i:
                yield text[i:nl]
                i = nl + 1
            else:
                yield text[i:j]
                i = j

    def _post_send_message(self, chat_id: str, text: str) -> bool:
        """
        Отправляет один кусок текста как plain-text без parse_mode.
        На ошибках пишет в лог и возвращает False.
        """
        data = parse.urlencode(
            {
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": "true",
                "disable_notification": "false",
            }
        ).encode("utf-8")

        req = request.Request(
            f"{self._base_url}/sendMessage",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self._timeout) as resp:
                # можно проверить код/тело при желании,
                # но для логов нам достаточно успешного ответа
                resp.read()
            return True
        except error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            log.error(
                "Telegram send failed: HTTP %s %s. Body=%r",
                getattr(e, "code", "?"),
                getattr(e, "reason", "?"),
                body,
            )
            return False
        except Exception as e:
            log.exception("Telegram send failed: %s", e)
            return False


class TelegramLogPump:
    """
    Неблокирующий насос логов:
    - складывает сообщения в очередь
    - отправляет по одному в фоне
    - не падает на ошибках отправки
    """

    def __init__(self, client: TelegramClient, *, to: str = "logs", max_queue: int = 1000) -> None:
        self._client = client
        self._to = to
        self._q: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=max_queue)
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name=f"telegram-pump-{self._to}")

    async def stop(self) -> None:
        if not self._task:
            return
        await self._q.put(None)
        try:
            await self._task
        finally:
            self._task = None

    async def send(self, text: str) -> None:
        try:
            self._q.put_nowait(text)
        except asyncio.QueueFull:
            # отбрасываем самое старое и вставляем новое
            try:
                _ = self._q.get_nowait()
            except Exception:
                pass
            await self._q.put(text)

    async def _run(self) -> None:
        while True:
            try:
                item = await self._q.get()
                if item is None:
                    break
                # отправляем синхронную часть в thread-пуле
                await asyncio.to_thread(self._client.send_text_sync, item, to=self._to)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("Telegram pump error: %s", e)


__all__ = ["TelegramClient", "TelegramLogPump"]
