import asyncio
import websockets
import json
import time
import redis.asyncio as redis
from strems import streams

redis_host = 'redis'
redis_port = 6380
redis_password = 'Redisbot1235!'
unique_symbols = set()
last_print_time = time.time()

class RedisClient:
    def __init__(self, host: str, port: int, password: str, db: int = 0):
        self.client = redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)

    async def save_closed_candle(self, candle: dict):
        key = f"candles1:{candle['symbol']}:{candle['interval']}"
        await self.client.rpush(key, json.dumps(candle))
        # print(f"Закрытая свеча сохранена в {key}")

    async def save_current_candle(self, candle: dict):
        key = f"current1:{candle['symbol']}:{candle['interval']}"
        await self.client.set(key, json.dumps(candle))


def rename_candle_keys(raw_candle: dict) -> dict:
    return {
        "open_time": raw_candle.get("t"),
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


async def subscribe_kline_streams():
    # Комбинированный URL для двух потоков:
    # btcusdt@kline_15m и ethusdt@kline_15m
    # url = "wss://fstream.binance.com/stream?streams=btcusdt@kline_1m/cosusdt@kline_1m"
    streams_part = "/".join(streams)
    url = f"wss://fstream.binance.com/stream?streams={streams_part}"
    redis_client = RedisClient(host=redis_host, port=redis_port, password=redis_password)
    last_print_time = time.time()
    unique_symbols = set()
    start_time = time.time()
    async with websockets.connect(url) as ws:
        print("Подключение установлено к Binance WebSocket")
        while True:
            try:
                message = await ws.recv()
                data = json.loads(message)
                # Пример вывода: можно обрабатывать полученные данные свечи
                raw_candle = data['data']['k']
                candle = rename_candle_keys(raw_candle)
                # unique_symbols.add(candle['symbol'])
                # if time.time() - last_print_time >= 60:
                #     print(f"За последнюю минуту получили {len(unique_symbols)} уникальных символов.")
                #     # Очищаем множество и сбрасываем таймер
                #     # unique_symbols.clear()
                #     last_print_time = time.time()

                if candle["is_closed"]:
                    await redis_client.save_closed_candle(candle)
                else:
                    await redis_client.save_current_candle(candle)
                    
            except Exception as e:
                print("Ошибка:", e)
                print(f"Отключение {start_time - time.time()}")
                break

if __name__ == '__main__':
    asyncio.run(subscribe_kline_streams())