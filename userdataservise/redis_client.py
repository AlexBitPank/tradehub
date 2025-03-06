# redis_client.py
import json
import redis.asyncio as redis
from decimal import Decimal
from datetime import datetime


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()  # Преобразуем datetime в строку формата ISO
        return super().default(obj)
    

class RedisClient:
    
    def __init__(self, host: str = None, port: int = None, password: str = None, db: int = 0):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.client = redis.Redis(host=self.host, port=self.port, db=self.db, password=self.password, decode_responses=True)
        # Добавляем локальный кэш настроек и блокировку для синхронизации доступа
        self._position_counters = {"LONG": 0, "SHORT": 0}
    
    # Методы для работы с позициями v2
    async def set_position(self, symbol: str, position_data: dict):
        """
        Обновляет позицию для заданного символа.
        Данные хранятся в хеше с ключом "positions:{symbol}"
        в поле, соответствующем значению positionSide (например, "LONG" или "SHORT").
        Также обновляет вспомогательные наборы open_long_positions / open_short_positions.
        """
        key = f"positions:{symbol}"
        side = position_data.get("positionSide")
        if not side:
            raise ValueError("positionSide отсутствует в данных позиции")
        
        # Сохраняем данные для нужной стороны в виде JSON-строки
        await self.client.hset(key, side, json.dumps(position_data, cls=DecimalEncoder))

        # Определяем, открыта ли позиция (positionAmt != 0)
        try:
            amt = float(position_data.get("positionAmt", "0"))
        except Exception:
            amt = 0

        if amt != 0:
            # Если позиция открыта, добавляем символ в соответствующий набор
            if side.upper() == "LONG":
                await self.client.sadd("open_long_positions", symbol)
            elif side.upper() == "SHORT":
                await self.client.sadd("open_short_positions", symbol)
            
        else:
            # Если позиция закрыта, удаляем символ из набора
            if side.upper() == "LONG":
                await self.client.srem("open_long_positions", symbol)
            elif side.upper() == "SHORT":
                await self.client.srem("open_short_positions", symbol)
        
        self._position_counters["LONG"] = await self.client.scard("open_long_positions")
        self._position_counters["SHORT"] = await self.client.scard("open_short_positions")

    async def get_position(self, symbol: str) -> dict:
        """
        Возвращает позиции для заданного символа.
        Данные возвращаются в виде словаря, где ключи – это "LONG" и/или "SHORT".
        Если для позиции нет данных, возвращается пустой словарь.
        """
        key = f"positions:{symbol}"
        data = await self.client.hgetall(key)
        if not data:
            return {}
        result = {}
        for side, json_data in data.items():
            try:
                result[side] = json.loads(json_data)
            except Exception:
                result[side] = None
        return result

    async def get_all_positions(self) -> dict:
        """
        Возвращает все позиции для всех символов.
        Ключи в возвращаемом словаре – это имена символов, значения – словари с позициями ("LONG", "SHORT").
        """
        positions = {}
        # Получаем все ключи, начинающиеся с "positions:"
        keys = await self.client.keys("positions:*")
        for key in keys:
            # Из ключа получаем символ (например, "positions:AAVEUSDT" -> "AAVEUSDT")
            symbol = key.split(":", 1)[1]
            data = await self.client.hgetall(key)
            if data:
                positions[symbol] = {side: json.loads(val) for side, val in data.items()}
        return positions

    async def get_all_open_positions(self) -> list:
        """
        Возвращает список всех открытых позиций (как LONG, так и SHORT).
        Используется объединение наборов open_long_positions и open_short_positions.
        """
        open_long = await self.client.smembers("open_long_positions")
        open_short = await self.client.smembers("open_short_positions")
        # Объединяем символы (если символ имеет обе открытые позиции, он будет возвращён один раз, поэтому
        # дополнительно проверяем каждую сторону)
        symbols = set(open_long) | set(open_short)
        open_positions = []
        for symbol in symbols:
            pos_data = await self.get_position(symbol)
            for side, data in pos_data.items():
                try:
                    if float(data.get("positionAmt", "0")) != 0:
                        open_positions.append(data)
                except Exception:
                    continue
        return open_positions

    async def get_all_open_long_positions(self) -> list:
        """
        Возвращает список всех открытых длинных позиций.
        Для каждого символа из набора open_long_positions извлекается значение поля "LONG".
        """
        symbols = await self.client.smembers("open_long_positions")
        open_long_positions = []
        for symbol in symbols:
            key = f"positions:{symbol}"
            data = await self.client.hget(key, "LONG")
            if data:
                try:
                    pos = json.loads(data)
                    # Дополнительная проверка на случай некорректных данных
                    if float(pos.get("positionAmt", "0")) != 0:
                        open_long_positions.append(pos)
                except Exception:
                    continue
        return open_long_positions

    async def get_all_open_short_positions(self) -> list:
        """
        Возвращает список всех открытых коротких позиций.
        Для каждого символа из набора open_short_positions извлекается значение поля "SHORT".
        """
        symbols = await self.client.smembers("open_short_positions")
        open_short_positions = []
        for symbol in symbols:
            key = f"positions:{symbol}"
            data = await self.client.hget(key, "SHORT")
            if data:
                try:
                    pos = json.loads(data)
                    if float(pos.get("positionAmt", "0")) != 0:
                        open_short_positions.append(pos)
                except Exception:
                    continue
        return open_short_positions
    
    async def get_open_long_count(self) -> int:
        """
        Возвращает количество открытых длинных позиций из локального кэша.
        """
        return self._position_counters.get("LONG", 0)

    async def get_open_short_count(self) -> int:
        """
        Возвращает количество открытых коротких позиций из локального кэша.
        """
        return self._position_counters.get("SHORT", 0)
    
    async def get_all_open_count(self) -> int:
        """
        Возвращает общее количество открытых позиций (сумма open_long и open_short),
        используя локальный кэш _position_counters.
        """
        return self._position_counters.get("LONG", 0) + self._position_counters.get("SHORT", 0)    
        
    # Методы для работы с ордерами v2
    async def set_order(self, order_data: dict):
        # print(f'set_order {order_data['symbol']}')
        """
        Сохраняет или обновляет данные ордера.
        Добавляет orderId в набор открытых ордеров и набор по symbol,
        если статус ордера открытый (NEW или PARTIALLY_FILLED).
        """
        order_id = order_data.get("orderId")
        if not order_id:
            raise ValueError("orderId отсутствует в данных ордера")
        
        # Используем ключ вида "orders:{order_id}"
        key = f"orders:{order_id}"
        
        # Преобразуем Decimal и bool в строку
        order_data_str = {
            k: str(v) if isinstance(v, (Decimal, bool)) else v for k, v in order_data.items()
        }
        await self.client.hset(key, mapping=order_data_str)

        symbol = order_data.get("symbol", "")
        status = order_data.get("status")
        
        # Если ордер открыт, добавляем его в наборы открытых ордеров
        if status in ("NEW", "PARTIALLY_FILLED"):
            await self.client.sadd("open_orders", order_id)
            if symbol:
                await self.client.sadd(f"symbol_orders:{symbol}", order_id)
        else:
            # Если ордер закрыт, удаляем его из наборов открытых ордеров
            await self.client.srem("open_orders", order_id)
            if symbol:
                await self.client.srem(f"symbol_orders:{symbol}", order_id)

    async def get_open_orders(self) -> list:
        # print(f'get_open_orders')
        """
        Получает все открытые ордера.
        """
        order_ids = await self.client.smembers("open_orders")
        orders = []
        for order_id in order_ids:
            key = f"orders:{order_id}"
            order = await self.client.hgetall(key)
            if order:
                orders.append(order)
        return orders

    async def get_open_orders_by_symbol(self, symbol: str) -> list:
        # print(f'get_open_orders_by_symbol {symbol}')
        """
        Получает открытые ордера для заданного символа.
        """
        order_ids = await self.client.smembers(f"symbol_orders:{symbol}")
        orders = []
        for order_id in order_ids:
            key = f"orders:{order_id}"
            order = await self.client.hgetall(key)
            if order:
                orders.append(order)
        return orders

    async def update_order(self, order_data: dict):
        # print(f'update_order {order_data['symbol']}')
        """
        Обновляет данные ордера.
        """
        await self.set_order(order_data)

    async def sync_open_orders(self, api_open_orders: list):
        # print(f'sync_open_orders')
        """
        Синхронизирует открытые ордера в Redis с данными, полученными через API.
        
        1. Для каждого ордера из API вызывается set_order (создается/обновляется запись).
        2. Получается текущее множество открытых ордеров в Redis.
        3. Если какой-либо ордер присутствует в Redis, но отсутствует в списке API,
           он удаляется из Redis (ключ и привязка к символу).
        """
        # Создаем множество order_id, полученных из API
        api_order_ids = set()
        for order in api_open_orders:
            order_id = order.get("orderId")
            if order_id:
                api_order_ids.add(str(order_id))
                await self.set_order(order)

        # Получаем множество order_id, которые уже зарегистрированы как открытые в Redis
        stored_order_ids = await self.client.smembers("open_orders")

        # Удаляем те ордера, которых больше нет в API
        for order_id in stored_order_ids:
            if order_id not in api_order_ids:
                key = f"orders:{order_id}"
                order = await self.client.hgetall(key)
                if order:
                    symbol = order.get("symbol", "")
                    await self.client.delete(key)
                    await self.client.srem("open_orders", order_id)
                    if symbol:
                        await self.client.srem(f"symbol_orders:{symbol}", order_id)
    
    async def get_filtered_orders(self, **kwargs) -> list:
        # print(f'get_filtered_orders {kwargs}')
        """
        Возвращает список ордеров, удовлетворяющих заданным фильтрам.
        Если в аргументах передан ключ 'symbol', сначала выбираются ордера для этого символа,
        иначе – извлекаются все открытые ордера.
        
        Фильтрация выполняется путем сравнения значений (приводится к строке).
        
        Пример использования:
        
            blending_orders = await redis_client.get_filtered_orders(
                symbol="BTCUSDT",
                status='NEW',
                side='BUY',
                type='LIMIT',
                positionSide=positionSide
            )
        """
        # Если передан symbol, извлекаем его и удаляем из фильтров
        symbol = kwargs.pop("symbol", None)
        if symbol:
            orders = await self.get_open_orders_by_symbol(symbol)
        else:
            orders = await self.get_open_orders()

        # Применяем дополнительную фильтрацию по оставшимся ключам
        for filter_key, filter_value in kwargs.items():
            orders = [order for order in orders if str(order.get(filter_key)) == str(filter_value)]
        return orders