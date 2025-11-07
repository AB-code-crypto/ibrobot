# history_form_IB.py
# Разовый скрипт: тянет из IB последние 24 часа 5-сек баров по localSymbol (по умолчанию MNQZ5),
# создаёт НОВУЮ SQLite-БД (или дозаписывает при --append) и пишет в таблицу bars_<symbol>
# только одну колонку времени (ts, epoch UTC) + OHLCV/average/barCount. Без отдельной таблицы инструментов.

import argparse
import asyncio
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import re
import sys
from typing import List, Tuple, Any

from ib_insync import IB, Future  # pip install ib-insync


# ---------- DB helpers ----------

def sanitize_symbol(symbol: str) -> str:
    return re.sub(r'[^A-Za-z0-9_]+', '_', symbol).strip('_')


def table_name_for_symbol(symbol: str) -> str:
    return f"bars_{sanitize_symbol(symbol).lower()}"


def ensure_pragmas(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA foreign_keys=ON;")
    conn.commit()


def ensure_instrument_table(conn: sqlite3.Connection, symbol: str) -> str:
    tname = table_name_for_symbol(symbol)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {tname} (
            ts        INTEGER PRIMARY KEY,   -- epoch UTC (шаг 5 сек)
            open      REAL    NOT NULL,
            high      REAL    NOT NULL,
            low       REAL    NOT NULL,
            close     REAL    NOT NULL,
            volume    REAL,
            average   REAL,
            barCount  INTEGER
        );
    """)
    return tname


def insert_bars(conn: sqlite3.Connection, symbol: str, rows: List[Tuple[Any, ...]]) -> int:
    """
    rows: список кортежей (ts, open, high, low, close, volume, average, barCount)
    """
    tname = ensure_instrument_table(conn, symbol)
    conn.executemany(
        f"INSERT OR IGNORE INTO {tname} "
        f"(ts, open, high, low, close, volume, average, barCount) "
        f"VALUES (?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return conn.execute(f"SELECT changes();").fetchone()[0] or 0


# ---------- IB helpers ----------

def to_epoch_utc(dt_like) -> int:
    """
    Приводим дату бара к epoch UTC (int).
    При formatDate=2 IB обычно отдаёт unix-время; покрываем и другие варианты на всякий случай.
    """
    # Уже число?
    if isinstance(dt_like, (int, float)):
        return int(dt_like)

    # datetime?
    if isinstance(dt_like, datetime):
        dt = dt_like if dt_like.tzinfo is not None else dt_like.replace(tzinfo=timezone.utc)
        return int(dt.astimezone(timezone.utc).timestamp())

    s = str(dt_like).strip()

    # Чисто числовая строка?
    if re.fullmatch(r"\d+(\.\d+)?", s):
        return int(float(s))

    # Формат 'YYYYMMDD  HH:MM:SS' (IB)
    if re.match(r"^\d{8}\s+\d{2}:\d{2}:\d{2}$", s):
        dt = datetime.strptime(s, "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())

    # ISO-подобный fallback
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())


async def fetch_5s_last_24h(ib: IB, local_symbol: str, use_rth: bool):
    """
    Возвращает (symbol, список BarData).
    Контракт строим по localSymbol (надежно для 'MNQZ5').
    """
    fut = Future(localSymbol=local_symbol, exchange="CME", currency="USD")
    qualified = await ib.qualifyContractsAsync(fut)
    if not qualified:
        raise RuntimeError(f"Не удалось квалифицировать контракт по localSymbol={local_symbol}")
    contract = qualified[0]
    symbol = contract.localSymbol or local_symbol

    bars = await ib.reqHistoricalDataAsync(
        contract=contract,
        endDateTime="",              # сейчас
        durationStr="1 D",           # последние 24 часа
        barSizeSetting="5 secs",
        whatToShow="TRADES",
        useRTH=use_rth,
        formatDate=2,                # ✅ UTC epoch от IB
        keepUpToDate=False
    )
    return symbol, bars


# ---------- Main ----------

async def amain(args):
    # Готовим БД: новая по умолчанию; если существует и не указан --append, удалим
    db_path = Path(args.db).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists() and not args.append:
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    ensure_pragmas(conn)

    ib = IB()
    try:
        await ib.connectAsync(args.host, args.port, clientId=args.client_id)

        symbol, bars = await fetch_5s_last_24h(ib, args.local_symbol, args.rth)

        rows: List[Tuple[Any, ...]] = []
        for b in bars:
            ts = to_epoch_utc(b.date)  # хранение только в одном виде: epoch UTC
            avg = (
                getattr(b, "average", None)
                or getattr(b, "wap", None)
                or getattr(b, "WAP", None)
                or 0.0
            )
            rows.append((
                ts,
                float(b.open),
                float(b.high),
                float(b.low),
                float(b.close),
                float(getattr(b, "volume", 0) or 0),
                float(avg or 0),
                int(getattr(b, "barCount", 0) or 0),
            ))

        inserted = insert_bars(conn, symbol, rows)
        # Небольшая сводка
        min_ts = min((r[0] for r in rows), default=None)
        max_ts = max((r[0] for r in rows), default=None)
        print(
            f"OK: контракт={symbol}, записей добавлено={inserted}, "
            f"диапазон UTC: {min_ts}..{max_ts}, БД={db_path}"
        )
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass
        conn.close()


def main():
    ap = argparse.ArgumentParser(
        description="Загрузка 5-сек баров за последние 24 часа из IB в SQLite (по localSymbol, напр. MNQZ5)."
    )
    ap.add_argument("--db", default="./data/ib_bars_intraday.sqlite", help="Путь к SQLite-БД")
    ap.add_argument("--local-symbol", default="MNQZ5", help="localSymbol контракта (например, MNQZ5)")
    ap.add_argument("--host", default="127.0.0.1", help="Хост TWS/IBG")
    ap.add_argument("--port", type=int, default=7496, help="Порт TWS/IBG")
    ap.add_argument("--client-id", type=int, default=101, help="Client ID")
    ap.add_argument("--rth", type=lambda s: s.lower() in {"1","true","yes","y","t"}, default=False,
                    help="useRTH (True/False), по умолчанию False (вся сессия)")
    ap.add_argument("--append", action="store_true",
                    help="Не удалять БД, если уже существует (дозапись в ту же схему)")
    args = ap.parse_args()

    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("Остановлено пользователем.", file=sys.stderr)
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
