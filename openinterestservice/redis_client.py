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
        """
        RPUSH + LTRIM, чтобы хранить только последние max_length записей.
        """
        if not oi_list:
            return
        key = f"open_interest:{symbol}"
        async with self.client.pipeline(transaction=False) as pipe:
            for item in oi_list:
                pipe.rpush(key, json.dumps(item))
            pipe.ltrim(key, -max_length, -1)
            await pipe.execute()

        logger.info(f"Сохранили {len(oi_list)} записей в Redis (list) для {symbol}, key={key}")

    async def list_oi_keys(self):
        """
        Для отладки: покажем все ключи open_interest:*
        """
        keys = await self.client.keys("open_interest:*")
        return keys
