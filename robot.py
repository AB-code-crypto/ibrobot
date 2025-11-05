from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ib_insync import IB
from core.config import LOGGING, IB_CONFIG, TELEGRAM
from core.telegram import TelegramLogPump, TelegramClient

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
    Делает небогатую, но полезную сводку из кэша ib_insync:
    аккаунт(ы), NetLiq/AvailableFunds (USD), кэш по валютам, позиции.
    Только plain-text, без угловых скобок и форматирования.
    """
    # Эти коллекции в ib_insync наполняются событием после connectAsync()
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
    for p in positions[:10]:  # не распыляемся
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


# ------------------------------- задачи: часовые маяки ----------------------------

async def _hour_beacons(pump: TelegramLogPump | None) -> None:
    if not pump or not TELEGRAM.enabled_logs:
        return
    # ждём ближайший верх часа
    now = datetime.now(TZ)
    next_top = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    await asyncio.sleep((next_top - now).total_seconds())
    while True:
        try:
            stamp = next_top.strftime("%Y-%m-%d %H:00")
            await pump.send(f"⏱ Начало часа: {stamp} (Europe/Tallinn)")
            # следующий час
            next_top = next_top + timedelta(hours=1)
            await asyncio.sleep( (next_top - datetime.now(TZ)).total_seconds() )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.getLogger("robot").exception("Часовой маяк: ошибка: %s", e)
            await asyncio.sleep(5)


# ------------------------------- задачи: охрана соединения ------------------------

async def _guard_connection(ib: IB, pump: TelegramLogPump | None) -> None:
    """
    Единственная задача, которая следит за соединением и переподключает.
    Без множества подписок на события, чтобы не словить гонки.
    """
    log = logging.getLogger("robot")
    base = IB_CONFIG.base_retry_delay
    maxd = IB_CONFIG.max_retry_delay
    period = IB_CONFIG.health_check_period

    backoff = base
    first_connect_done = False

    while True:
        try:
            if not ib.isConnected():
                msg = "🔗 Подключаюсь к IB ..."
                log.info(msg)
                if pump and TELEGRAM.enabled_logs:
                    await pump.send(msg)

                try:
                    await ib.connectAsync(IB_CONFIG.host, IB_CONFIG.port, IB_CONFIG.client_id)
                except Exception as e:
                    log.warning("Не удалось подключиться: %s", e)
                    if pump and TELEGRAM.enabled_logs:
                        await pump.send(f"⚠️ Подключение не удалось: {e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 1.7, maxd)
                    continue

                # Подключились
                backoff = base
                stamp = _now()
                ok_msg = f"✅ Подключено к IB {IB_CONFIG.host}:{IB_CONFIG.port} (clientId={IB_CONFIG.client_id}) в {stamp}"
                log.info(ok_msg)
                if pump and TELEGRAM.enabled_logs:
                    await pump.send(ok_msg)

                # Дать IB чуть времени на первичную синхронизацию
                await asyncio.sleep(1.0)

                if not first_connect_done:
                    snap = _compose_startup_snapshot(ib)
                    log.info(snap.replace("\n", " | "))
                    if pump and TELEGRAM.enabled_logs:
                        await pump.send(snap)
                    first_connect_done = True
            else:
                # живём, просто ждём
                await asyncio.sleep(period)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("Сторож соединения: ошибка: %s", e)
            await asyncio.sleep(2)


# ------------------------------- основная корутина --------------------------------

async def amain() -> None:
    _setup_logging()
    log = logging.getLogger("robot")
    log.info("🚀 Робот стартует. Лог-уровень: %s", LOGGING.level.upper())

    # Телеграм (plain-text). Насос логов включаем только если разрешено.
    tg_client = TelegramClient()
    pump: TelegramLogPump | None = None
    if TELEGRAM.enabled_logs:
        pump = TelegramLogPump(tg_client, to="logs", max_queue=1000)
        pump.start()
        await pump.send("✈️ Телеграм-логгер активирован")

    ib = IB()

    # Доп. сообщения при разрыве/закрытии (без подписок на множество событий)
    async def _graceful_disconnect():
        try:
            if ib.isConnected():
                log.info("🔌 Отключаюсь от IB ...")
                await asyncio.to_thread(ib.disconnect)  # безопасно вынести в thread
        finally:
            if pump:
                await pump.send("=======================\n🔚 Соединение закрыто\n=======================")
            if pump:
                await pump.stop()

    # Параллельные задачи: охрана соединения и часовые маяки
    guard_task = asyncio.create_task(_guard_connection(ib, pump), name="ib-guard")
    beacons_task = asyncio.create_task(_hour_beacons(pump), name="hour-beacons")

    # Ожидаем Ctrl+C или падение задач
    try:
        await asyncio.gather(guard_task, beacons_task)
    except asyncio.CancelledError:
        pass
    finally:
        await _graceful_disconnect()


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        # красивый выход
        logging.getLogger("robot").info("✅ Робот завершил работу корректно.")


if __name__ == "__main__":
    main()
