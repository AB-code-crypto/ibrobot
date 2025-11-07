#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Импорт истории из CSV (IB формат) в SQLite.
Структура на диске ожидается:
./history/mnq/
    MNQM5/
        MNQM5_2025-06-20.csv
        ...
    MNQU5/
        ...
    MNQZ5/
        ...

Для каждого инструмента создаётся СВОЯ таблица вида bars_<symbol.lower()> с колонками:
    ts INTEGER PRIMARY KEY  -- epoch UTC (шаг 5 сек)
    open, high, low, close REAL
    volume REAL
    average REAL
    barCount INTEGER

Пример запуска:
  python import_history.py --root ./history/mnq --db ./data/ib_bars.sqlite --fresh
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import re
import sys
from typing import Iterable, Tuple, List


# --------- Helpers ---------

def sanitize_symbol(symbol: str) -> str:
    """Оставляем буквы/цифры/подчёркивание; остальное -> '_'."""
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
    """
    Создаёт (если нужно) таблицу для инструмента.
    """
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


def parse_iso_to_epoch_utc(iso_like: str) -> int:
    """
    'date' из IB CSV обычно вида: 'YYYY-MM-DD HH:MM:SS+03:00' или с иным оффсетом.
    Преобразуем к epoch UTC (секунды).
    """
    dt = datetime.fromisoformat(iso_like)  # понимает смещения вида +03:00
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def human_utc(ts: int | None) -> str:
    if ts is None:
        return "—"
    # без DeprecationWarning:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(sep=" ")


def walk_history(root: Path) -> Iterable[Tuple[str, List[Path]]]:
    """
    Возвращает пары: (SYMBOL, [CSV файлы]).
    """
    for instrument_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        symbol = instrument_dir.name
        csv_files = sorted(instrument_dir.glob("*.csv"))
        if csv_files:
            yield symbol, csv_files


# --------- Import ---------

def import_csv_into_table(conn: sqlite3.Connection, symbol: str, csv_path: Path) -> int:
    """
    Импорт одного CSV-файла в таблицу инструмента.
    Возвращает количество попыток вставки (дубликаты тихо игнорируются).
    """
    tname = ensure_instrument_table(conn, symbol)

    batch: list[tuple] = []
    inserted_total = 0

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # В CSV из IB обычно есть хотя бы эти колонки:
        required = ("date", "open", "high", "low", "close")
        for r in required:
            if r not in reader.fieldnames:
                raise RuntimeError(
                    f"{csv_path.name}: отсутствует обязательная колонка '{r}'. "
                    f"Найдены: {reader.fieldnames}"
                )

        for row in reader:
            try:
                ts = parse_iso_to_epoch_utc(row["date"])
                open_ = float(row["open"])
                high_ = float(row["high"])
                low_ = float(row["low"])
                close = float(row["close"])
                volume = float(row.get("volume", 0) or 0)
                average = float(row.get("average", 0) or 0)
                barCount = int(float(row.get("barCount", 0) or 0))

                batch.append((ts, open_, high_, low_, close, volume, average, barCount))

                if len(batch) >= 20_000:
                    conn.executemany(
                        f"INSERT OR IGNORE INTO {tname} "
                        f"(ts, open, high, low, close, volume, average, barCount) "
                        f"VALUES (?,?,?,?,?,?,?,?)",
                        batch
                    )
                    conn.commit()
                    inserted_total += len(batch)
                    batch.clear()

            except Exception as e:
                print(f"[WARN] Пропуск строки {csv_path.name}: {e}", file=sys.stderr)

    if batch:
        conn.executemany(
            f"INSERT OR IGNORE INTO {tname} "
            f"(ts, open, high, low, close, volume, average, barCount) "
            f"VALUES (?,?,?,?,?,?,?,?)",
            batch
        )
        conn.commit()
        inserted_total += len(batch)

    return inserted_total


def summarize_table(conn: sqlite3.Connection, symbol: str) -> tuple[int | None, int | None, int]:
    tname = table_name_for_symbol(symbol)
    cur = conn.execute(f"SELECT MIN(ts), MAX(ts), COUNT(*) FROM {tname};")
    row = cur.fetchone()
    if not row:
        return None, None, 0
    return row[0], row[1], row[2]


# --------- Main ---------

def main():
    ap = argparse.ArgumentParser(
        description="Импорт CSV истории IB в SQLite. Одна таблица на инструмент, одна колонка времени (ts, UTC epoch)."
    )
    ap.add_argument("--root", default="./history/mnq", help="Корневая папка истории (по умолчанию ./history/mnq)")
    ap.add_argument("--db", default="./data/ib_bars.sqlite", help="Путь к SQLite базе (по умолчанию ./data/ib_bars.sqlite)")
    ap.add_argument("--fresh", action="store_true", help="Если указан — удалить существующую БД перед созданием")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    db_path = Path(args.db).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if args.fresh and db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    try:
        ensure_pragmas(conn)

        total_files = 0
        total_rows = 0

        for symbol, files in walk_history(root):
            print(f"\n=== {symbol} ===")
            tname = ensure_instrument_table(conn, symbol)
            print(f"Таблица: {tname}")

            for csv_path in files:
                total_files += 1
                print(f"  -> {csv_path.name} ... ", end="", flush=True)
                inserted = import_csv_into_table(conn, symbol, csv_path)
                total_rows += inserted
                print(f"OK (+{inserted})")

            # сводка по инструменту
            min_ts, max_ts, cnt = summarize_table(conn, symbol)
            print(f"Итог {symbol}: строк={cnt}, диапазон UTC: {human_utc(min_ts)} .. {human_utc(max_ts)}")

        print("\nГотово.")
        print(f"Файлов обработано: {total_files}")
        print(f"Всего строк прочитано/вставлено (попыток): {total_rows}")
        print(f"БД: {db_path}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
