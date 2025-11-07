from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from ib_insync import IB, Future, BarData

# ==========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================

MONTH_SEQ = ["H", "M", "U", "Z"]  # Mar / Jun / Sep / Dec


def _parse_local_symbol(local_symbol: str) -> Tuple[str, str, str]:
    """
    Разбирает локальный символ IB фьючерса квартальных серий, например:
    'MNQZ5' -> ('MNQ', 'Z', '5')
    'ESM6'  -> ('ES',  'M', '6')
    """
    local_symbol = local_symbol.strip().upper()
    # Найти последнюю букву из H/M/U/Z
    idx = max(local_symbol.rfind(m) for m in MONTH_SEQ)
    if idx <= 0 or idx == len(local_symbol) - 1:
        raise ValueError(f"Не удалось разобрать localSymbol: {local_symbol}")
    root = local_symbol[:idx]
    mon = local_symbol[idx]
    year_tail = local_symbol[idx + 1:]
    if not year_tail:
        raise ValueError(f"Нет годового хвоста у localSymbol: {local_symbol}")
    return root, mon, year_tail


def _neighbors_quarter(local_symbol: str) -> Tuple[str, str]:
    """
    Для активного квартального контракта вернуть (prev, next).
    Пример: 'MNQZ5' -> ('MNQU5', 'MNQH6')
    """
    root, mon, year_tail = _parse_local_symbol(local_symbol)
    i = MONTH_SEQ.index(mon)
    prev_mon = MONTH_SEQ[(i - 1) % 4]
    next_mon = MONTH_SEQ[(i + 1) % 4]

    # Годовые хвосты: после Z -> H следующего года; перед H -> U того же года-1
    year_int = int(year_tail)
    prev_year = year_int if mon != "H" else year_int  # для U5 -> H5 (тот же хвост)
    next_year = year_int if mon != "Z" else year_int + 1

    prev_sym = f"{root}{prev_mon}{year_int if mon != 'H' else year_int}"  # хвост тот же
    next_sym = f"{root}{next_mon}{next_year}"
    # Нюанс: для prev от 'H' логично вернуть 'U' того же года-1, но у нас квартальные идут по кругу:
    # H -> prev = U того же года? В IB местами используют один символ года.
    # На практике, если у вас 'MNQH6', предыдущий — 'MNQU5'. Исправим строго:
    if mon == "H":
        prev_sym = f"{root}U{year_int - 1}"
    if mon == "Z":
        next_sym = f"{root}H{year_int + 1}"

    return prev_sym, next_sym


def _dt_to_epoch_seconds(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _to_epoch(bar_date) -> int:
    """
    ib_insync.BarData.date может быть datetime либо int (при formatDate=2).
    Приводим к int (UTC seconds).
    """
    if isinstance(bar_date, (int, float)):
        return int(bar_date)
    if isinstance(bar_date, datetime):
        # Считаем, что это UTC (при formatDate=2 IB отдаёт UTC)
        return _dt_to_epoch_seconds(bar_date)
    # fallback
    return int(float(bar_date))


# ==========================
# НАСТРОЙКИ ДЖОБЫ
# ==========================

@dataclass(frozen=True)
class BarsCollectorConfig:
    db_path: str
    active_local_symbol: str  # например 'MNQZ5'
    poll_interval_sec: float = 5.0  # базовая частота «дозагрузки»
    short_window_seconds: int = 40  # окно для инкрементальных запросов
    backfill_max_days: int = 10  # максимально добирать прошлое до N дней
    use_rth: bool = False  # regular trading hours только? по умолчанию нет
    what_to_show: str = "TRADES"  # тип данных
    exchange: str = "CME"  # для MNQ
    currency: str = "USD"  # для MNQ


# ==========================
# ОСНОВНОЙ КЛАСС
# ==========================

class BarsCollector:
    """
    Асинхронная задача: ведёт 5-сек бары в SQLite для активного фьючерса и его соседей.
    """

    def __init__(self, ib: IB, cfg: BarsCollectorConfig, logger=None):
        self.ib = ib
        self.cfg = cfg
        self.log = logger
        self._running = False
        self._conn = sqlite3.connect(self.cfg.db_path, isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")

        self._tracked: Dict[str, dict] = {}  # symbol -> {last_ts:int|None}
        self._setup_tracked_set()

    # ---------- Публичное API ----------

    async def run(self):
        """
        Главный цикл: бэекфилл + инкрементальное пополнение.
        """
        self._running = True
        # Бэекфилл для всех отслеживаемых в момент старта
        for sym in list(self._tracked.keys()):
            try:
                await self._ensure_table(sym)
                await self._backfill_from_db_tail(sym)
            except Exception as e:
                self._log_warn(f"[{sym}] backfill error: {e}")

        # Основной инкрементальный цикл
        while self._running:
            # Актуализировать набор (вдруг активный сменился вне класса — переинициализация не требуется,
            # но раз в петлю можно безопасно пересчитать соседей)
            self._refresh_neighbors()

            tasks = [self._incremental_fetch(sym) for sym in list(self._tracked.keys())]
            # Выполним по очереди, чтобы не ловить лимиты IB — оставить параллелизм на 1–2 можно, но консервативно
            for coro in tasks:
                try:
                    await coro
                except Exception as e:
                    self._log_warn(f"incremental fetch error: {e}")

            # Спим до следующего "кратно 5 сек" момента
            await self._sleep_to_next_5s()

    async def stop(self):
        self._running = False
        try:
            await asyncio.sleep(0)  # дать корутинам корректно завершиться
        finally:
            with closing(self._conn):
                self._conn.close()

    # ---------- Внутренние методы ----------

    def _setup_tracked_set(self):
        active = self.cfg.active_local_symbol.upper()
        prev_sym, next_sym = _neighbors_quarter(active)
        for sym in (prev_sym, active, next_sym):
            if sym not in self._tracked:
                self._tracked[sym] = {"last_ts": self._get_db_last_ts(sym)}

    def _refresh_neighbors(self):
        # Пересобираем тройку вокруг активного: если уже есть — оставим «last_ts»
        active = self.cfg.active_local_symbol.upper()
        prev_sym, next_sym = _neighbors_quarter(active)
        desired = {prev_sym, active, next_sym}

        # Удаляем лишние
        for sym in list(self._tracked.keys()):
            if sym not in desired:
                self._tracked.pop(sym, None)

        # Добавляем недостающие
        for sym in desired:
            if sym not in self._tracked:
                self._tracked[sym] = {"last_ts": self._get_db_last_ts(sym)}

    async def _ensure_table(self, symbol: str):
        sql = f"""
        CREATE TABLE IF NOT EXISTS "{symbol}" (
            ts      INTEGER PRIMARY KEY,  -- UTC seconds
            open    REAL NOT NULL,
            high    REAL NOT NULL,
            low     REAL NOT NULL,
            close   REAL NOT NULL,
            volume  REAL NOT NULL,
            wap     REAL NOT NULL,
            count   INTEGER NOT NULL
        );
        """
        await asyncio.to_thread(self._conn.executescript, sql)

    def _get_db_last_ts(self, symbol: str) -> Optional[int]:
        try:
            cur = self._conn.execute(f'SELECT MAX(ts) FROM "{symbol}"')
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
        except sqlite3.OperationalError:
            # Таблицы ещё нет
            return None

    async def _backfill_from_db_tail(self, symbol: str):
        """
        Дотягиваем историю от последнего ts в БД до "сейчас".
        Ограничиваемся cfg.backfill_max_days.
        """
        last_ts = self._get_db_last_ts(symbol)
        await self._ensure_table(symbol)

        now_utc = datetime.now(timezone.utc)
        if last_ts is None:
            # Если данных нет — по умолчанию поднимем до 1 суток (или backfill_max_days— на твой вкус)
            start_dt = now_utc - timedelta(days=min(1, self.cfg.backfill_max_days))
        else:
            start_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc) + timedelta(seconds=5)

        min_dt = now_utc - timedelta(days=self.cfg.backfill_max_days)
        if start_dt < min_dt:
            start_dt = min_dt

        await self._fetch_and_store_range(symbol, start_dt, now_utc)

    async def _incremental_fetch(self, symbol: str):
        """
        Подтягивает небольшое «скользящее» окно назад от текущего момента,
        чтобы гарантированно захватить недодавленные бары.
        """
        await self._ensure_table(symbol)

        now_utc = datetime.now(timezone.utc)
        end_dt = now_utc
        duration_sec = max(30, self.cfg.short_window_seconds)
        start_dt = end_dt - timedelta(seconds=duration_sec)

        bars = await self._req_bars(symbol, end_dt=end_dt, duration_str=f"{duration_sec} S")
        if not bars:
            # Для «следующего» контракта в начале жизни это нормально
            return

        rows = []
        for b in bars:
            ts = _to_epoch(b.date)
            rows.append((ts, b.open, b.high, b.low, b.close, b.volume, getattr(b, "wap", 0.0), getattr(b, "barCount", 0)))

        if rows:
            await asyncio.to_thread(self._bulk_insert_ignore, symbol, rows)
            new_tail = max(ts for ts, *_ in rows)
            self._tracked[symbol]["last_ts"] = max(self._tracked[symbol]["last_ts"] or 0, new_tail)

    async def _fetch_and_store_range(self, symbol: str, start_dt: datetime, end_dt: datetime):
        """
        Дотягиваем историю оконно, чтобы не упираться в лимиты.
        """
        await self._ensure_table(symbol)

        # IB допускает большие окна, но надёжнее идти батчами по 1 дню / 6 часов / 30 минут.
        # Возьмём 1-дневные окна для простоты.
        step = timedelta(days=1)
        cur_end = start_dt + step
        if cur_end > end_dt:
            cur_end = end_dt

        while cur_end <= end_dt + timedelta(seconds=1):
            duration = int((cur_end - start_dt).total_seconds())
            duration = max(duration, 30)
            bars = await self._req_bars(symbol, end_dt=cur_end, duration_str=f"{duration} S")
            if bars:
                rows = []
                for b in bars:
                    ts = _to_epoch(b.date)
                    # Вставим только то, что >= старту (окно двигаем вперёд)
                    if ts >= _to_epoch(start_dt):
                        rows.append(
                            (ts, b.open, b.high, b.low, b.close, b.volume, getattr(b, "wap", 0.0), getattr(b, "barCount", 0))
                        )
                if rows:
                    await asyncio.to_thread(self._bulk_insert_ignore, symbol, rows)
                    self._tracked[symbol]["last_ts"] = max(self._tracked[symbol]["last_ts"] or 0, max(ts for ts, *_ in rows))

            # следующее окно
            start_dt = cur_end + timedelta(seconds=1)
            cur_end = start_dt + step
            if cur_end > end_dt:
                cur_end = end_dt

            # чтобы не спамить IB — маленькая пауза
            await asyncio.sleep(0.2)

    async def _req_bars(self, symbol: str, end_dt: Optional[datetime], duration_str: str) -> List[BarData]:
        """
        Единичный запрос истории для localSymbol.
        """
        contract = Future(
            localSymbol=symbol,
            exchange=self.cfg.exchange,
            currency=self.cfg.currency,
        )
        bars: List[BarData] = await self.ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime=end_dt or "",
            durationStr=duration_str,
            barSizeSetting="5 secs",
            whatToShow=self.cfg.what_to_show,
            useRTH=self.cfg.use_rth,
            formatDate=2,  # epoch seconds (UTC)
            keepUpToDate=False,
        )
        return bars or []

    def _bulk_insert_ignore(self, symbol: str, rows: Sequence[Tuple[int, float, float, float, float, float, float, int]]):
        self._conn.executemany(
            f'INSERT OR IGNORE INTO "{symbol}" (ts, open, high, low, close, volume, wap, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            rows,
        )

    async def _sleep_to_next_5s(self):
        now = datetime.now(timezone.utc)
        next_slot = (int(now.timestamp()) // 5 + 1) * 5
        await asyncio.sleep(max(0.0, next_slot - now.timestamp()))

    def _log_info(self, msg: str):
        if self.log:
            self.log.info(msg)

    def _log_warn(self, msg: str):
        if self.log:
            self.log.warning(msg)
