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

    async def save_open_interest(self, symbol: str, oi_data: list):
        """
        Сохраняем OI-данные в Redis. 
        Простейший вариант – просто хранить JSON в ключе open_interest:<symbol>.
        """
        key = f"open_interest:{symbol}"
        data_str = json.dumps(oi_data)
        await self.client.set(key, data_str)
        # При желании можно добавить expire:
        # await self.client.expire(key, 300)  # expire = 5 минут или другое время
        logger.info(f"Сохранили OI в Redis key={key}")
