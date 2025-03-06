import os
import asyncio
import logging
import aiohttp

from strems import streams
from redis_client import RedisClient
from db import DBManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")  # Лучше задать "redis" по умолчанию
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

DB_HOST = os.getenv("DB_HOST", "mariadb")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mypass")
DB_NAME = os.getenv("DB_NAME", "open_interest_db")

OPEN_INTEREST_URL = "https://fapi.binance.com/futures/data/openInterestHist"
PERIOD = "5m"
DEFAULT_LIMIT = 10
MAX_REDIS_LIST_LEN = 10  # Храним в Redis последние 10

def parse_symbol_from_stream(stream_name: str) -> str:
    base_symbol = stream_name.split('@')[0]
    return base_symbol.upper()

async def fetch_open_interest(session, symbol: str, start_ts=None, limit=DEFAULT_LIMIT) -> list:
    params = {
        "symbol": symbol,
        "period": PERIOD,
        "limit": limit
    }
    if start_ts is not None:
        params["startTime"] = start_ts
    try:
        async with session.get(OPEN_INTEREST_URL, params=params) as response:
            response.raise_for_status()
            data = await response.json()
            return data
    except Exception as e:
        logger.error(f"Ошибка при запросе OI для {symbol}: {e}")
        return []

async def process_symbol_oi(symbol: str, session, redis_client: RedisClient, db_manager: DBManager):
    last_ts = await redis_client.get_last_timestamp(symbol)
    data = await fetch_open_interest(session, symbol, start_ts=last_ts + 1 if last_ts else None)
    if not data:
        logger.info(f"Нет новых данных OI для {symbol}")
        return

    # Фильтруем всё, что <= last_ts
    if last_ts:
        new_data = [d for d in data if d.get("timestamp", 0) > last_ts]
    else:
        new_data = data

    if not new_data:
        logger.info(f"Нет новых OI-записей для {symbol} после timestamp={last_ts}")
        return

    logger.info(f"Получено {len(new_data)} новых OI-записей для {symbol}")

    # Сохраняем в БД
    await db_manager.save_open_interest(symbol, new_data)

    # Сохраняем в Redis (list)
    await redis_client.push_open_interest_list(symbol, new_data, max_length=MAX_REDIS_LIST_LEN)

    # Обновляем last_timestamp
    max_ts = max(d.get("timestamp", 0) for d in new_data)
    await redis_client.save_last_timestamp(symbol, max_ts)
    logger.info(f"Обновлён last_timestamp для {symbol} = {max_ts}")

async def collect_and_store_open_interest():
    logger.info("Начинаем сбор OI по всем символам")

    redis_client = RedisClient(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
    db_manager = DBManager(host=DB_HOST, port=DB_PORT,
                           user=DB_USER, password=DB_PASSWORD, db_name=DB_NAME)

    # Повторяем попытки подключения к БД, если нужно
    await asyncio.sleep(3)
    for attempt in range(10):
        try:
            await db_manager.init_pool()
            break
        except Exception as e:
            logger.warning(f"MariaDB не готова? Попытка {attempt+1}/10: {e}")
            await asyncio.sleep(3)

    async with aiohttp.ClientSession() as session:
        # Запускаем задачи для всех стримов параллельно
        tasks = []
        for stream in streams:
            symbol = parse_symbol_from_stream(stream)
            tasks.append(process_symbol_oi(symbol, session, redis_client, db_manager))
        await asyncio.gather(*tasks)

    await db_manager.close_pool()

async def main():
    while True:
        await collect_and_store_open_interest()
        # 5 минут паузы
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
