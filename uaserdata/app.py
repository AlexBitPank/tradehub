import asyncio
import websockets
import json
import time
import redis.asyncio as redis
from strems import streams

redis_host = 'redis'
redis_port = 6379
redis_password = 'Redisbot1235!'

class RedisClient:
    def __init__(self, host: str, port: int, password: str, db: int = 0):
        self.client = redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)

    async def save_closed_candle_in_list(self, candle: dict):
        """
        Сохраняет "закрывшуюся" свечу в конец списка (RPUSH), 
        одновременно обрезая список (LTRIM), 
        чтобы в нём оставалось не более 50 последних свечей.
        """
        key = f"candles:{candle['symbol']}:{candle['interval']}"
        # Используем пайплайн, чтобы отправить несколько команд одним пакетом
        async with self.client.pipeline(transaction=False) as pipe:
            pipe.rpush(key, json.dumps(candle))
            pipe.ltrim(key, -5, -1)  # Оставляем последние 5 элементов
            await pipe.execute()

    async def save_closed_candle(self, candle: dict):
        """
        Сохраняем каждую закрытую свечу в отдельном ключе с TTL = 1 час.
        Дополнительно добавляем запись в отсортированный набор (Sorted Set),
        чтобы можно было получить список ключей свечей в хронологическом порядке.
        """
        # Формируем уникальный ключ, например по close_time
        # Пример: candle:BTCUSDT:1m:1677801600000
        candle_key = f"candle:{candle['symbol']}:{candle['interval']}:{candle['close_time']}"

        # Сохраняем данные свечи
        await self.client.set(candle_key, json.dumps(candle))

        # Устанавливаем время жизни ключа (TTL) = 3600 секунд (1 час)
        await self.client.expire(candle_key, 1600)

        # Добавляем ключ свечи в Sorted Set с ключом вида:
        # candles_index:BTCUSDT:1m
        # В качестве score используем close_time (float).
        zset_key = f"candles_index:{candle['symbol']}:{candle['interval']}"
        close_time_score = float(candle['close_time'])
        await self.client.zadd(zset_key, {candle_key: close_time_score})

    async def save_current_candle(self, candle: dict):
        key = f"current:{candle['symbol']}:{candle['interval']}"
        await self.client.set(key, json.dumps(candle))


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
        print("Ошибка при обработке сообщения:", e)

async def receive_messages(ws, redis_client: RedisClient):
    async for message in ws:
        # Создаем отдельную задачу для обработки сообщения
        asyncio.create_task(process_message(message, redis_client))

async def subscribe_kline_streams():
    # url = "wss://fstream.binance.com/stream?streams=btcusdt@kline_5m/cosusdt@kline_5m"
    # Формируем URL, объединяя элементы списка streams через "/"
    streams_part = "/".join(streams)
    url = f"wss://fstream.binance.com/stream?streams={streams_part}"
    redis_client = RedisClient(host=redis_host, port=redis_port, password=redis_password)
    
    while True:
        try:
            # Возможно, имеет смысл увеличить ping_timeout, если сервер ожидает быстрее
            async with websockets.connect(url, ping_interval=180, ping_timeout=600) as ws:
                print("Подключение к Binance WebSocket установлено")
                await receive_messages(ws, redis_client)
        except Exception as e:
            print("Ошибка соединения:", e)
        print("Переподключаемся через 5 секунд...")
        await asyncio.sleep(5)

if __name__ == '__main__':
    asyncio.run(subscribe_kline_streams())
