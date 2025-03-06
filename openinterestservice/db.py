import aiomysql
import logging

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self, host, port, user, password, db_name):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.pool = None

    async def init_pool(self):
        """
        Инициализирует пул подключений к MariaDB.
        Заодно создаёт базу (если не существует), 
        а затем внутри неё — таблицу open_interest_history.
        """
        # Сначала подключимся к mysql-серверу без указания db, 
        # чтобы создать db при необходимости
        try:
            logger.info("Подключаемся к MariaDB без указания базы для initial setup...")
            conn = await aiomysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
            async with conn.cursor() as cur:
                # Создаём базу данных, если не существует
                await cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
            conn.close()
        except Exception as e:
            logger.error(f"Ошибка при создании базы {self.db_name}: {e}")

        # Теперь создаём пул подключений к нужной базе
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db_name,
            autocommit=True,
            minsize=1,
            maxsize=5
        )

        # Создаём таблицу open_interest_history
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS open_interest_history (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timestamp BIGINT NOT NULL,
            sum_open_interest VARCHAR(50),
            sum_open_interest_value VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_table_sql)
        logger.info("Таблица open_interest_history проверена/создана")

    async def close_pool(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def save_open_interest(self, symbol: str, oi_data: list):
        """
        Сохраняем записи OI в таблицу open_interest_history.
        oi_data – список словарей, например:
        [
          {
            "symbol": "BTCUSDT",
            "sumOpenInterest": "123.456",
            "sumOpenInterestValue": "12345.67",
            "timestamp": 1677801600000
          },
          ...
        ]
        """
        if not oi_data:
            return

        insert_sql = """
            INSERT INTO open_interest_history 
            (symbol, timestamp, sum_open_interest, sum_open_interest_value)
            VALUES (%s, %s, %s, %s)
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for oi in oi_data:
                    ts = oi.get("timestamp", 0)
                    sum_oi = oi.get("sumOpenInterest", None)
                    sum_oi_val = oi.get("sumOpenInterestValue", None)
                    await cur.execute(
                        insert_sql,
                        (symbol, ts, sum_oi, sum_oi_val)
                    )
