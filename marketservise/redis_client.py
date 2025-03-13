
import json
import redis.asyncio as redis


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
        key = f"candle_current:{candle['symbol']}:{candle['interval']}"
        await self.client.set(key, json.dumps(candle))
