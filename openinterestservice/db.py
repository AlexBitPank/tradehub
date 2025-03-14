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
        try:
            logger.info(f"Подключаемся к MariaDB без указания базы ({self.host}:{self.port})...")
            conn = await aiomysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
            async with conn.cursor() as cur:
                await cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
            conn.close()
        except Exception as e:
            logger.error(f"Ошибка при создании базы {self.db_name}: {e}")

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

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS open_interest_history (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timestamp BIGINT NOT NULL,
            sum_open_interest VARCHAR(50),
            sum_open_interest_value VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        unique_index_sql = """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_timestamp
        ON open_interest_history (symbol, timestamp);
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_table_sql)
                try:
                    await cur.execute(unique_index_sql)
                except Exception as e:
                    logger.warning(f"Не удалось создать уникальный индекс (возможно, уже существует): {e}")
        logger.info("Таблица open_interest_history и индекс проверены/созданы")

        # Создаём новую таблицу для логов
        create_log_table_sql = """
        CREATE TABLE IF NOT EXISTS open_interest_log (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            log_message TEXT,
            log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_log_table_sql)
        logger.info("Таблица open_interest_log проверена/создана")


    async def close_pool(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def save_open_interest(self, symbol: str, oi_data: list):
        if not oi_data:
            return

        insert_sql = """
            INSERT IGNORE INTO open_interest_history
            (symbol, timestamp, sum_open_interest, sum_open_interest_value)
            VALUES (%s, %s, %s, %s)
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for oi in oi_data:
                    ts = oi.get("timestamp", 0)
                    sum_oi = oi.get("sumOpenInterest", "")
                    sum_oi_val = oi.get("sumOpenInterestValue", "")
                    try:
                        await cur.execute(insert_sql, (symbol, ts, sum_oi, sum_oi_val))
                    except Exception as e:
                        logger.error(f"Ошибка при вставке OI (symbol={symbol}, ts={ts}): {e}")

    async def get_last_timestamp(self, symbol: str) -> int:
        """
        Возвращает максимальный (последний) timestamp для заданного символа из БД.
        """
        query = "SELECT MAX(timestamp) FROM open_interest_history WHERE symbol = %s"
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (symbol,))
                row = await cur.fetchone()
                return row[0] if row and row[0] is not None else None

    async def get_open_interest_timestamps(self, symbol: str, limit: int = 10) -> list:
        """
        Возвращает список timestamp последних записей для заданного символа из БД.
        """
        query = "SELECT timestamp FROM open_interest_history WHERE symbol = %s ORDER BY timestamp DESC LIMIT %s"
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (symbol, limit))
                rows = await cur.fetchall()
                timestamps = [row[0] for row in rows if row[0] is not None]
                return timestamps

  # Новый метод для сохранения логов
    async def save_log(self, log_message: str):
        insert_sql = """
            INSERT INTO open_interest_log (log_message)
            VALUES (%s)
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(insert_sql, (log_message,))
                except Exception as e:
                    logger.error(f"Ошибка при вставке лога: {e}")