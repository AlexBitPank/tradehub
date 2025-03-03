import asyncio
import logging
import websockets
import json
import time
import redis.asyncio as redis
from strems import streams

# -----------------------------
# Настройка логирования
# -----------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Вывод в консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
# Формат логов: время, уровень логирования, сообщение
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# -----------------------------
# Константы и настройки
# -----------------------------
REDIS_HOST = 'redis'
REDIS_PORT = 6380
REDIS_PASSWORD = 'Redisbot1235!'
REDIS_DB = 0

# Максимальное число задач-«воркеров», одновременно обрабатывающих очередь
WORKERS_COUNT = 5

# Максимальный размер очереди (если хотите ограничить буфер)
# Если очередь заполнена, новые сообщения будут дожидаться освобождения
MAX_QUEUE_SIZE = 1000

# Используем для подсчёта уникальных символов (по желанию)
unique_symbols = set()
last_print_time = time.time()
UNIQUE_SYMBOLS_LOG_INTERVAL = 300  # Раз в 60 секунд логируем кол-во уникальных символов

# -----------------------------
# Класс для работы с Redis
# -----------------------------
class RedisClient:
    def __init__(self, host: str, port: int, password: str, db: int = 0):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        # Объект Redis (async)
        self.client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            decode_responses=True
        )

    async def check_connection(self) -> bool:
        """
        Простая проверка доступности Redis.
        Возвращает True, если Redis ответил на ping, иначе False.
        """
        try:
            pong = await self.client.ping()
            return pong is True
        except Exception as exc:
            logger.error(f"Redis недоступен: {exc}")
            return False

    async def save_closed_candle(self, candle: dict):
        key = f"candles:{candle['symbol']}:{candle['interval']}"
        await self.client.rpush(key, json.dumps(candle))

    async def save_current_candle(self, candle: dict):
        key = f"current:{candle['symbol']}:{candle['interval']}"
        await self.client.set(key, json.dumps(candle))


# -----------------------------
# Функция для переименования ключей свечи
# -----------------------------
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


# -----------------------------
# Обработка одного сообщения
# -----------------------------
async def process_message(message: str, redis_client: RedisClient):
    global unique_symbols, last_print_time
    try:
        data = json.loads(message)
        raw_candle = data['data']['k']
        candle = rename_candle_keys(raw_candle)

        # Работа со статистикой уникальных символов
        unique_symbols.add(candle['symbol'])
        current_time = time.time()
        if current_time - last_print_time >= UNIQUE_SYMBOLS_LOG_INTERVAL:
            logger.info(f"За последние {UNIQUE_SYMBOLS_LOG_INTERVAL} c получили {len(unique_symbols)} уникальных символов.")
            # при желании можно делать очистку set, если это необходимо
            unique_symbols.clear()
            last_print_time = current_time

        # Сохранение в Redis
        if candle["is_closed"]:
            await redis_client.save_closed_candle(candle)
        else:
            await redis_client.save_current_candle(candle)

    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")


# -----------------------------
# Воркер, который обрабатывает очередь
# -----------------------------
async def worker(queue: asyncio.Queue, redis_client: RedisClient, worker_id: int):
    """
    Воркер читает из очереди и вызывает process_message для каждого сообщения.
    """
    while True:
        message = await queue.get()
        try:
            await process_message(message, redis_client)
        except Exception as exc:
            logger.error(f"[Worker {worker_id}] Ошибка при обработке: {exc}")
        finally:
            queue.task_done()


# -----------------------------
# Функция чтения сообщений из WebSocket
# -----------------------------
async def receive_messages(ws, queue: asyncio.Queue):
    """
    Получаем сообщения из WebSocket и складываем их в очередь.
    """
    async for message in ws:
        # Добавляем в очередь. Если очередь заполнена (MAX_QUEUE_SIZE), 
        # то этот вызов будет ждать, пока не освободится место.
        await queue.put(message)


# -----------------------------
# Основная функция подписки и переподключения
# -----------------------------
async def subscribe_kline_streams():
    """
    Подключаемся к WebSocket Binance, создаём очередь, запускаем воркеров для обработки сообщений.
    Реализуем переподключение с экспоненциальной задержкой.
    """
    # Создаём RedisClient
    redis_client = RedisClient(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB
    )

    # Сначала проверим доступность Redis
    if not await redis_client.check_connection():
        logger.warning("Redis недоступен при запуске. Будем всё равно пытаться работать и переподключаться...")

    # Готовим очередь и группу воркеров
    queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

    # Запускаем пуул воркеров
    for i in range(WORKERS_COUNT):
        asyncio.create_task(worker(queue, redis_client, i))

    # Формируем URL, объединяя элементы списка streams через "/"
    streams_part = "/".join(streams)
    url = f"wss://fstream.binance.com/stream?streams={streams_part}"

    reconnect_attempt = 0  # счётчик попыток переподключения

    while True:
        try:
            # Экспоненциальная задержка
            if reconnect_attempt > 0:
                # например, лимитируем 30 сек максимальной задержкой
                delay = min(2 ** reconnect_attempt, 30)
                logger.info(f"Ожидание {delay} секунд перед переподключением...")
                await asyncio.sleep(delay)

            # logger.info(f"Подключаемся к WebSocket: {url}")
            logger.info(f"Подключаемся к WebSocket")
            async with websockets.connect(url, ping_interval=180, ping_timeout=600) as ws:
                logger.info("Подключение к Binance WebSocket установлено")
                reconnect_attempt = 0  # сбросим, так как успешно подключились

                # Запускаем корутину чтения сообщений
                await receive_messages(ws, queue)

        except Exception as e:
            logger.error(f"Ошибка соединения или чтения WebSocket: {e}")
            reconnect_attempt += 1

            # Проверяем, доступен ли Redis в новом цикле
            await redis_client.check_connection()
            logger.info("Будем пытаться переподключиться...")


# -----------------------------
# Точка входа
# -----------------------------
if __name__ == '__main__':
    try:
        asyncio.run(subscribe_kline_streams())
    except KeyboardInterrupt:
        logger.info("Остановка скрипта по KeyboardInterrupt")
