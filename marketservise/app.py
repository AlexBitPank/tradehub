from os import getenv
import asyncio
import websockets
import json
import redis.asyncio as redis
from strems import streams
import logging
from redis_client import RedisClient

REDIS_HOST = getenv("REDIS_HOST")
REDIS_PORT = getenv("REDIS_PORT")
REDIS_PASSWORD = getenv("REDIS_PASSWORD")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def rename_candle_keys(raw_candle: dict) -> dict:
    return {
        "start_time": raw_candle.get("t"),
        "close_time": raw_candle.get("T"),
        "symbol": raw_candle.get("s"),
        "interval": raw_candle.get("i"),
        "first_trade_id": raw_candle.get("f"),
        "last_trade_id": raw_candle.get("L"),
        "open": raw_candle.get("o"),
        "close": raw_candle.get("c"),
        "high": raw_candle.get("h"),
        "low": raw_candle.get("l"),
        "volume": raw_candle.get("v"),
        "number_of_trades": raw_candle.get("n"),
        "is_closed": raw_candle.get("x"),
        "quote_volume": raw_candle.get("q"),
        "taker_buy_base_volume": raw_candle.get("V"),
        "taker_buy_quote_volume": raw_candle.get("Q")
    }

async def process_message(message: str, redis_client: RedisClient):
    
    try:
        data = json.loads(message)
        raw_candle = data['data']['k']
        candle = rename_candle_keys(raw_candle)
        
        if candle["is_closed"]:
            await redis_client.save_closed_candle_in_list(candle)
        else:
            await redis_client.save_current_candle(candle)
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")

async def receive_messages(ws, redis_client: RedisClient):
    async for message in ws:
        # Создаем отдельную задачу для обработки сообщения
        asyncio.create_task(process_message(message, redis_client))

async def subscribe_kline_streams():
    # url = "wss://fstream.binance.com/stream?streams=btcusdt@kline_5m/cosusdt@kline_5m"
    # Формируем URL, объединяя элементы списка streams через "/"
    streams_part = "/".join(streams)
    url = f"wss://fstream.binance.com/stream?streams={streams_part}"
    redis_client = RedisClient(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
    
    while True:
        try:
            # Возможно, имеет смысл увеличить ping_timeout, если сервер ожидает быстрее
            async with websockets.connect(url, ping_interval=180, ping_timeout=600) as ws:
                # logger.info(f"Подключение к Binance WebSocket установлено")
                print(f"Подключение к Binance WebSocket установлено")
                await receive_messages(ws, redis_client)
        except Exception as e:
            logger.error(f"Ошибка соединения: {e}")
        logger.info(f"Переподключаемся через 5 секунд...")
        await asyncio.sleep(5)

if __name__ == '__main__':
    asyncio.run(subscribe_kline_streams())
