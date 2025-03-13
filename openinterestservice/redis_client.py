import json
import redis.asyncio as redis
import logging

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self, host: str, port: int, password: str, db: int = 0):
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )

    async def get_last_timestamp(self, symbol: str) -> int:
        key = f"open_interest_last_ts:{symbol}"
        value = await self.client.get(key)
        if value is not None:
            return int(value)
        return None

    async def save_last_timestamp(self, symbol: str, timestamp: int):
        key = f"open_interest_last_ts:{symbol}"
        await self.client.set(key, timestamp)

    async def push_open_interest_list(self, symbol: str, oi_list: list, max_length=10):
        if not oi_list:
            return
        key = f"open_interest:{symbol}"
        async with self.client.pipeline(transaction=False) as pipe:
            for item in oi_list:
                pipe.rpush(key, json.dumps(item))
            pipe.ltrim(key, -max_length, -1)
            await pipe.execute()
        # logger.info(f"Сохранили {len(oi_list)} записей в Redis (list) для {symbol}, key={key}")

    async def get_open_interest_list(self, symbol: str):
        key = f"open_interest:{symbol}"
        data = await self.client.lrange(key, 0, -1)
        return data

    async def list_oi_keys(self):
        keys = await self.client.keys("open_interest:*")
        return keys

    # Новые методы для current OI:
    async def save_current_open_interest(self, symbol: str, data: dict):
        """
        Сохраняем текущий OI в Redis по ключу open_interest_current:{symbol}.
        """
        key = f"open_interest_current:{symbol}"
        await self.client.set(key, json.dumps(data))

    async def get_current_open_interest(self, symbol: str):
        """
        Получаем текущий OI из Redis. Если нет данных, вернется None.
        """
        key = f"open_interest_current:{symbol}"
        raw = await self.client.get(key)
        if raw:
            return json.loads(raw)
        return None