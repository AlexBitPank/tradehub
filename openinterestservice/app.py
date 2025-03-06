import os
import asyncio
import logging
import json
import aiohttp

from strems import streams  # Ваш список стримов (пример: ['btcusdt@kline_5m', 'ethusdt@kline_5m', ...]
from redis_client import RedisClient
from db import DBManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Считываем переменные окружения
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

DB_HOST = os.getenv("DB_HOST", "mariadb")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mypass")
DB_NAME = os.getenv("DB_NAME", "open_interest_db")

# URL для запроса открытого интереса
OPEN_INTEREST_URL = "https://fapi.binance.com/futures/data/openInterestHist"
PERIOD = "5m"
LIMIT = 10

def parse_symbol_from_stream(stream_name: str) -> str:
    """
    Пример: 'btcusdt@kline_5m' -> 'BTCUSDT'
    """
    base_symbol = stream_name.split('@')[0]
    return base_symbol.upper()

async def fetch_open_interest(session, symbol: str) -> list:
    """
    Запрашиваем OI-данные для symbol через aiohttp (асинхронно).
    """
    params = {
        "symbol": symbol,
        "period": PERIOD,
        "limit": LIMIT
    }
    try:
        async with session.get(OPEN_INTEREST_URL, params=params) as response:
            response.raise_for_status()
            data = await response.json()
            return data  # Список словарей
    except Exception as e:
        logger.error(f"Ошибка при запросе OI для {symbol}: {e}")
        return []

async def collect_and_store_open_interest():
    """
    Основная задача:
      1) Пройтись по всем стримам,
      2) Получить OI для каждого символа,
      3) Сохранить в Redis (кэш),
      4) Сохранить в MariaDB (история).
    """
    logger.info("Начинаем сбор OI по всем символам")
    redis_client = RedisClient(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
    db_manager = DBManager(host=DB_HOST, port=DB_PORT, 
                           user=DB_USER, password=DB_PASSWORD, db_name=DB_NAME)

    # Инициализируем пул подключений к MariaDB
    await db_manager.init_pool()

    # Открываем aiohttp.ClientSession для всех запросов
    async with aiohttp.ClientSession() as session:
        for stream in streams:
            symbol = parse_symbol_from_stream(stream)
            oi_data = await fetch_open_interest(session, symbol)
            if oi_data:
                logger.info(f"Получены OI-данные для {symbol}: {oi_data}")
                await redis_client.save_open_interest(symbol, oi_data)
                await db_manager.save_open_interest(symbol, oi_data)
            else:
                logger.warning(f"Нет данных OI для {symbol}")

    # Закрываем пул подключений к MariaDB
    await db_manager.close_pool()

async def main():
    """
    Запускаем периодический сбор OI с интервалом, например, 300 секунд.
    """
    while True:
        await collect_and_store_open_interest()
        await asyncio.sleep(300)  # Пауза 5 минут

if __name__ == "__main__":
    asyncio.run(main())
