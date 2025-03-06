# user_data_app.py
from os import getenv
import asyncio
import json
import logging
from decimal import Decimal
from typing import Optional

import websockets
import requests
from redis_client import RedisClient

API_KEY_BIN = getenv("API_KEY_BIN")
REDIS_HOST = getenv("REDIS_HOST")
REDIS_PORT = getenv("REDIS_PORT")
REDIS_PASSWORD = getenv("REDIS_PASSWORD")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def get_listen_key(api_key: str, base_url: str = "https://fapi.binance.com") -> str:
    """
    Получает listenKey для USDS-M фьючерсов (функция синхронная, 
    если нужно асинхронно – перепишите под aiohttp/httpx).
    """
    url = f"{base_url}/fapi/v1/listenKey"
    headers = {
        "X-MBX-APIKEY": api_key
    }
    try:
        response = requests.post(url, headers=headers, timeout=10)
    except requests.RequestException as e:
        logger.error("Ошибка сети при получении listenKey: %s", e)
        raise RuntimeError("Сетевая ошибка при запросе listenKey") from e

    if response.status_code != 200:
        logger.error(
            "Неуспешный статус %s при получении listenKey, тело ответа: %s",
            response.status_code, response.text
        )
        raise RuntimeError(f"Не удалось получить listenKey, статус: {response.status_code}")

    data = response.json()
    lk = data.get("listenKey")
    if not lk:
        logger.error("listenKey отсутствует в ответе Binance: %s", data)
        raise RuntimeError("Ответ Binance не содержит listenKey")

    logger.info("Успешно получен listenKey: %s", lk)
    return lk

# -----------------------------------------------------------------------------
# Заглушка для примера (обновление баланса)
# -----------------------------------------------------------------------------
class OFN:
    async def update_balance(self, asyncClient, g_balances):
        # Вставьте сюда свою реальную логику обновления баланса
        pass

ofn = OFN()
g_balances = {}  # условно глобальный словарь балансов, если нужно

# -----------------------------------------------------------------------------
# Обработка событий User Data Stream (ACCOUNT_UPDATE, ORDER_TRADE_UPDATE, ...)
# -----------------------------------------------------------------------------
async def process_user_data_event(res: dict, redis_client: RedisClient):
    """
    Обработка события от user data stream (фьючерсов).
    Пример логики: ACCOUNT_UPDATE / ORDER_TRADE_UPDATE.
    """
    event_type = res.get("e", "")
    if event_type == "ACCOUNT_UPDATE":
        # Пример обновления баланса
        await ofn.update_balance(None, g_balances)

        positions = res["a"]["P"]  # список позиций
        for row in positions:
            position_data = {
                "symbol": row["s"],
                "positionAmt": row["pa"],
                "entryPrice": row["ep"],
                "unrealizedProfit": row["up"],
                "updateTime": res["T"],
                "positionSide": row["ps"]
            }
            await redis_client.set_position(row["s"], position_data)

            # Для примера можем логировать размер в USD
            try:
                size_usd = Decimal(position_data["entryPrice"]) * Decimal(position_data["positionAmt"])
            except Exception:
                size_usd = Decimal(0)
            logger.info(
                "ACCOUNT_UPDATE %s price: %s size: %s (%.0f$)",
                position_data["symbol"],
                position_data["entryPrice"],
                position_data["positionAmt"],
                size_usd
            )

    if event_type == "ORDER_TRADE_UPDATE":
        order = res["o"]
        price = Decimal(order.get("p", 0.0))
        avgPrice = Decimal(order.get("ap", 0.0))
        origQty = Decimal(order.get("q", 0.0))
        cumQuote = price * origQty if price > 0 else avgPrice * origQty

        order_data = {
            "orderId": order.get("i", ""),
            "symbol": order.get("s", ""),
            "status": order.get("X", ""),
            "clientOrderId": order.get("c", ""),
            "price": price,
            "avgPrice": avgPrice,
            "origQty": origQty,
            "cumQuote": cumQuote,
            "type": order.get("o", ""),
            "side": order.get("S", ""),
            "positionSide": order.get("ps", ""),
            "stopPrice": order.get("sp", "0.0"),
            "workingType": order.get("wt", ""),
            "updateTime": res.get("T", ""),
            "goodTillDate": res.get("gtd", ""),
            "timeInForce": res.get("f", "")
        }

        await redis_client.update_order(order_data)
        logger.info("ORDER_TRADE_UPDATE: %s", order_data)


# -----------------------------------------------------------------------------
# Чтение сообщений (events) из WebSocket и передача в process_user_data_event
# -----------------------------------------------------------------------------
async def receive_user_data_messages(ws, redis_client: RedisClient):
    async for message in ws:
        try:
            data = json.loads(message)
            # Если используем прямое подключение: wss://fstream.binance.com/ws/<listenKey>,
            # то событие приходит напрямую: {"e":"ACCOUNT_UPDATE",...}
            # Если используем combined stream, то может прийти {"stream":"...", "data":{...}}
            # Предположим, что у нас прямое подключение (не combined), тогда:
            event_payload = data

            # Если нужно, проверяем "data" внутри:
            # if "data" in data:
            #     event_payload = data["data"]

            await process_user_data_event(event_payload, redis_client)
        except Exception as e:
            logger.error("Ошибка при обработке сообщения user data stream: %s", e, exc_info=True)


# -----------------------------------------------------------------------------
# Основная корутина для подписки к user data stream
# -----------------------------------------------------------------------------
async def subscribe_user_data_stream(api_key: str):
    """
    1. Получаем listenKey
    2. Подключаемся по WebSocket: wss://fstream.binance.com/ws/<listenKey>
    3. Слушаем события, при ошибке переподключаемся
    """
    # Получаем listenKey (через sync-функцию).
    # Если нужно, можно добавлять логику keep-alive, 
    # повторно вызывать get_listen_key или PUT-запросы каждые ~30 минут.
    try:
        listen_key = get_listen_key(api_key)
    except Exception as e:
        logger.error("Невозможно получить listenKey: %s", e)
        return  # или raise

    url = f"wss://fstream.binance.com/ws/{listen_key}"
    # Инициализируем RedisClient
    redis_client = RedisClient(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD
    )

    while True:
        try:
            async with websockets.connect(
                url, 
                ping_interval=180, 
                ping_timeout=600
            ) as ws:
                logger.info("Подключено к %s", url)
                await receive_user_data_messages(ws, redis_client)
        except Exception as e:
            logger.error("Ошибка соединения или чтения WebSocket: %s", e)
        
        logger.info("Переподключение через 5 секунд...")
        await asyncio.sleep(5)


if __name__ == "__main__":
    # Настройка логгера (вывод в консоль). 
    # В реальном проекте можно настроить иначе (в файл, JSON-формат и т.п.)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    # Запускаем основной цикл подписки
    asyncio.run(subscribe_user_data_stream(API_KEY_BIN))
