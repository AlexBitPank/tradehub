import warnings
warnings.filterwarnings("ignore", category=Warning, message="Duplicate entry.*for key 'idx_symbol_timestamp'")
warnings.filterwarnings("ignore", category=Warning, message="Can't create database.*")
warnings.filterwarnings("ignore", category=Warning, message="Table 'open_interest_history' already exists")
warnings.filterwarnings("ignore", category=Warning, message="Duplicate key name 'idx_symbol_timestamp'")

import os
import asyncio
import logging
import aiohttp
import time
import json

from strems import streams
from redis_client import RedisClient
from db import DBManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

DB_HOST = os.getenv("DB_HOST", "mariadb")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mypass")
DB_NAME = os.getenv("DB_NAME", "open_interest_db")

OPEN_INTEREST_HIST_URL = "https://fapi.binance.com/futures/data/openInterestHist"
CURRENT_OPEN_INTEREST_URL = "https://fapi.binance.com/fapi/v1/openInterest"

PERIOD = "5m"
DEFAULT_LIMIT = 30  # Анализируем последние 30 записей

def parse_symbol_from_stream(stream_name: str) -> str:
    base_symbol = stream_name.split('@')[0]
    return base_symbol.upper()

def shorten_number(number) -> str:
    if number < 1000:
        return str(number)
    elif number < 1_000_000:
        return f"{number / 1_000:.1f}K"
    else:
        return f"{number / 1_000_000:.1f}M"

async def clear_redis_data_on_startup(redis_client: RedisClient):
    """
    Удаляем ключи (open_interest:* и open_interest_current:*),
    чтобы при запуске скрипта не использовать старые данные.
    """
    try:
        # Удаляем исторические OI
        hist_keys = await redis_client.client.keys("open_interest:*")
        if hist_keys:
            await redis_client.client.delete(*hist_keys)
            logger.info(f"Удалено {len(hist_keys)} ключей исторических OI")

        # Удаляем текущие OI
        current_keys = await redis_client.client.keys("open_interest_current:*")
        if current_keys:
            await redis_client.client.delete(*current_keys)
            logger.info(f"Удалено {len(current_keys)} ключей текущих OI")

    except Exception as e:
        logger.error(f"Ошибка при очистке Redis: {e}")

async def fetch_open_interest(session, symbol: str, limit=DEFAULT_LIMIT) -> list:
    params = {
        "symbol": symbol,
        "period": PERIOD,
        "limit": limit
    }
    try:
        async with session.get(OPEN_INTEREST_HIST_URL, params=params) as response:
            response.raise_for_status()
            data = await response.json()
            return data
    except Exception as e:
        logger.error(f"Ошибка при запросе исторических данных OI для {symbol}: {e}")
        return []

async def fetch_current_open_interest(session, symbol: str) -> dict:
    params = {"symbol": symbol}
    try:
        async with session.get(CURRENT_OPEN_INTEREST_URL, params=params) as response:
            response.raise_for_status()
            data = await response.json()
            return data
    except Exception as e:
        logger.error(f"Ошибка при запросе текущего OI для {symbol}: {e}")
        return {}

async def _fetch_current_open_interest(
    session, 
    symbol: str, 
    max_retries: int = 3, 
    base_delay: float = 1.0
) -> dict:
    """
    Запрашивает текущий OI с API Binance, делая несколько попыток (retry)
    при таймаутах или сетевых ошибках. 
    Возвращает словарь с данными (или пустой, если не удалось).
    """
    params = {"symbol": symbol}
    
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(CURRENT_OPEN_INTEREST_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    # Например, 404 -> символ не найден, 429 -> rate limit, 5xx -> ошибка сервера и т.д.
                    error_text = await response.text()
                    logger.error(
                        f"Ошибка при запросе текущего OI для {symbol} "
                        f"(HTTP {response.status}, попытка {attempt}/{max_retries}): {error_text}"
                    )
                    return {}
        except asyncio.TimeoutError:
            # logger.error(
            #     f"Таймаут запроса текущего OI для {symbol} (попытка {attempt}/{max_retries})"
            # )
            # Если это последняя попытка, возвращаемся с пустым словарём (или можно raise).
            if attempt == max_retries:
                return {}
            # Иначе ждём и пробуем снова
            await asyncio.sleep(base_delay * attempt)
        except aiohttp.ClientConnectorError as e:
            logger.error(
                f"Сетевая ошибка (ClientConnectorError) для {symbol} "
                f"(попытка {attempt}/{max_retries}): {e}"
            )
            if attempt == max_retries:
                return {}
            await asyncio.sleep(base_delay * attempt)
        except Exception as e:
            logger.error(
                f"Неизвестная ошибка при запросе текущего OI для {symbol} "
                f"(попытка {attempt}/{max_retries}): {e}"
            )
            return {}
    
    return {}

async def _fetch_open_interest(
    session, 
    symbol: str, 
    period: str = "5m", 
    limit: int = 30,
    max_retries: int = 3, 
    base_delay: float = 1.0
) -> list:
    """
    Запрашивает исторические данные OI (openInterestHist) с API Binance, делая несколько попыток.
    Возвращает список (или пустой, если не удалось).
    """
    params = {
        "symbol": symbol,
        "period": period,
        "limit": limit
    }

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(OPEN_INTEREST_HIST_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    error_text = await response.text()
                    logger.error(
                        f"Ошибка при запросе исторических OI для {symbol} "
                        f"(HTTP {response.status}, попытка {attempt}/{max_retries}): {error_text}"
                    )
                    return []
        except asyncio.TimeoutError:
            # logger.error(
            #     f"Таймаут при запросе исторических OI для {symbol} "
            #     f"(попытка {attempt}/{max_retries})"
            # )
            if attempt == max_retries:
                return []
            await asyncio.sleep(base_delay * attempt)
        except aiohttp.ClientConnectorError as e:
            logger.error(
                f"Сетевая ошибка (ClientConnectorError) для {symbol} "
                f"(попытка {attempt}/{max_retries}): {e}"
            )
            if attempt == max_retries:
                return []
            await asyncio.sleep(base_delay * attempt)
        except Exception as e:
            logger.error(
                f"Неизвестная ошибка при запросе исторических OI для {symbol} "
                f"(попытка {attempt}/{max_retries}): {e}"
            )
            return []
    
    return []

async def process_symbol_current_oi(symbol: str, session, redis_client: RedisClient, db_manager: DBManager):
    """
    1) Запрашиваем текущий OI с биржи.
    2) Сохраняем в Redis через redis_client.save_current_open_interest.
    3) Получаем из Redis историю (до 30 записей), вычисляем средний OI и сравниваем c текущим.
    """
    try:
        data = await fetch_current_open_interest(session, symbol)
        # data = await fetch_current_open_interest(session, symbol, max_retries=3, base_delay=1.0)
        if not data:
            return
        
        # Сохраняем текущий OI, используя новый метод
        await redis_client.save_current_open_interest(symbol, data)
        
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            # Получаем список исторических данных (до 30 записей)
            hist_data = await redis_client.get_open_interest_list(symbol)
            
            # Убедимся, что у нас ровно DEFAULT_LIMIT (30) записей
            if len(hist_data) >= DEFAULT_LIMIT:
                # Суммируем поле sumOpenInterest
                analyz_data = hist_data[-DEFAULT_LIMIT:]
                sum_list = []
                for item_str in analyz_data:
                    item = json.loads(item_str)
                    sum_list.append(float(item["sumOpenInterest"]))

                avg_oi = sum(sum_list) / len(sum_list)
                
                # Текущее значение открытого интереса
                current_oi = float(data["openInterest"])
                
                # Читаем «пороговое» значение из Redis (если нет, используем 0)
                last_threshold_key = f"last_threshold:{symbol}"
                threshold_str = await redis_client.client.get(last_threshold_key)
                last_threshold = float(threshold_str) if threshold_str else None

                # Проверка отклонения более чем на n%
                if (last_threshold is None or current_oi > last_threshold) and current_oi > avg_oi * 1.1:
                    message = (
                        f"{symbol}: "
                        f"current={shorten_number(current_oi)}, avg({DEFAULT_LIMIT})={shorten_number(avg_oi)}, "
                        f"deviation={((current_oi - avg_oi) / avg_oi * 100):.2f}%"
                    )
                    logger.warning(message)
                    # Запись лога в БД
                    await db_manager.save_log(message)
                    new_threshold = current_oi * 1.01
                    await redis_client.client.set(last_threshold_key, new_threshold, ex=300)

                elif (last_threshold is None or current_oi < last_threshold) and current_oi < avg_oi * 0.9:
                    message = (
                        f"{symbol}: "
                        f"current={shorten_number(current_oi)}, avg({DEFAULT_LIMIT})={shorten_number(avg_oi)}, "
                        f"deviation={((current_oi - avg_oi) / avg_oi * 100):.2f}%"
                    )
                    logger.warning(message)
                    # Запись лога в БД
                    await db_manager.save_log(message)
                    new_threshold = current_oi * 0.99
                    await redis_client.client.set(last_threshold_key, new_threshold, ex=600)
                break                
            else:
                if attempt == max_retries:
                    logger.info(
                        f"Недостаточно данных для анализа ({len(hist_data)}/{DEFAULT_LIMIT}) для {symbol}"
                    )
                else:
                    # logger.error(
                    #     f"Нет данных для анализа {symbol} "
                    #     f"(попытка {attempt}/{max_retries})"
                    # )
                    await asyncio.sleep(3)

    except Exception as e:
        logger.error(f"Ошибка в process_symbol_current_oi для {symbol}: {e}")

async def process_symbol_hist(symbol: str, session, redis_client: RedisClient):
    try:
        data = await fetch_open_interest(session, symbol, limit=DEFAULT_LIMIT)
        # data = await fetch_open_interest(session, symbol, PERIOD, DEFAULT_LIMIT, max_retries=3, base_delay=1.0)
        if not data:
            return
        await redis_client.push_open_interest_list(symbol, data, max_length=DEFAULT_LIMIT)
    except Exception as e:
        logger.error(f"Ошибка в process_symbol_hist для {symbol}: {e}")

async def current_oi_loop(session, redis_client: RedisClient, db_manager: DBManager):
    logger.info("current_oi_loop started")
    while True:
        try:
            tasks = []
            for stream in streams:
                symbol = parse_symbol_from_stream(stream)
                tasks.append(process_symbol_current_oi(symbol, session, redis_client, db_manager))
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Ошибка в цикле сбора текущего OI: {e}")
        await asyncio.sleep(60)  # Каждую минуту

async def historical_oi_loop(session, redis_client: RedisClient):
    logger.info("historical_oi_loop started")
    while True:
        try:
            tasks = []
            for stream in streams:
                symbol = parse_symbol_from_stream(stream)
                tasks.append(process_symbol_hist(symbol, session, redis_client))
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Ошибка в цикле сбора исторического OI: {e}")
        await asyncio.sleep(300)  # Каждые 5 минут

async def main():
    redis_client = RedisClient(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
    # Очистка данных при старте
    await clear_redis_data_on_startup(redis_client)
    # Подключение к БД: создаём экземпляр DBManager и инициализируем пул соединений
    db_manager = DBManager(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        db_name=DB_NAME
    )
    await db_manager.init_pool()

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await asyncio.gather(
            current_oi_loop(session, redis_client, db_manager),
            historical_oi_loop(session, redis_client)
        )

if __name__ == "__main__":
    asyncio.run(main())
